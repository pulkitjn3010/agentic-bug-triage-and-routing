import asyncio
import os
from uuid import uuid4

import structlog
from fastapi import HTTPException

from ..kafka_client import publish_triage_request
from orchestrator.db.repositories.audit_log import get_last_triage_by_case_id
from orchestrator.db.repositories.source_registry import get_enabled_sources
from orchestrator.db.session import AsyncSessionLocal
from orchestrator.redis_client import get_cached_case_result

log = structlog.get_logger()


async def start_triage(
    bug_id: str,
    source_id: str,
    user_id: str,
    force_refresh: bool,
    kafka_producer,  # extracted from request.app.state by the route — never imported here
) -> dict:
    log.info("Triage request received", ticket_id=bug_id, source_id=source_id)

    async with AsyncSessionLocal() as db:
        sources = await get_enabled_sources(db)

    if source_id:
        # Validate that provided source_id exists
        valid_ids = {s.source_id for s in sources}
        if source_id not in valid_ids:
            source_id = ""  # fall through to detection

    if not source_id:
        # Server-side source detection: prefix match first
        for src in sources:
            prefix = (src.ticket_prefix or "").upper()
            if prefix and bug_id.upper().startswith(prefix + "-"):
                source_id = src.source_id
                break
        # For numeric IDs without prefix, use first GitHub connector
        if not source_id:
            for src in sources:
                if src.system_type == "github" and bug_id.isdigit():
                    source_id = src.source_id
                    break
        # Last resort: first enabled source
        if not source_id and sources:
            source_id = sources[0].source_id

    if not source_id:
        raise HTTPException(status_code=400, detail="No source system configured")

    log.info("Starting triage", ticket_id=bug_id, source_id=source_id, user=user_id)

    # Short-circuit logic: Check if ANY engineer has triaged this recently and it's still in Redis cache.
    if not force_refresh:
        from sqlalchemy import select, desc
        from orchestrator.db.models import AuditLog
        from orchestrator.db.repositories.audit_log import insert_audit_entry
        
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(AuditLog)
                .where(AuditLog.bug_id == bug_id, AuditLog.step == "pipeline_complete")
                .order_by(desc(AuditLog.created_at))
                .limit(1)
            )
            last_global_triage = result.scalar_one_or_none()
            
            if last_global_triage and last_global_triage.case_id:
                # Check if it's still warm in the Redis cache
                cached = await get_cached_case_result(last_global_triage.case_id)
                if cached:
                    # Cache hit! Create a new AuditLog row for THIS user so they get it in their independent history
                    await insert_audit_entry(db, {
                        "case_id": last_global_triage.case_id,
                        "bug_id": bug_id,
                        "source_id": source_id,
                        "engineer_id": user_id,
                        "step": "pipeline_complete",
                        "status": "done",
                        "summary": last_global_triage.summary,
                        "systems_queried": last_global_triage.systems_queried,
                        "duration_ms": 1100  # Fast simulated duration
                    })
                    log.info("Triage cache hit", bug_id=bug_id, user_id=user_id, case_id=last_global_triage.case_id)
                    return {"case_id": last_global_triage.case_id, "bug_id": bug_id, "source_id": source_id, "status": "cached"}

    case_id = str(uuid4())

    # Always run pipeline locally when fallback is enabled
    enable_fallback = (
        os.getenv("ENABLE_LOCAL_PIPELINE_FALLBACK", "true").lower() == "true"
    )

    if enable_fallback:
        from orchestrator.orchestrator import TaskOrchestrator

        orch = TaskOrchestrator()
        asyncio.create_task(
            orch.run(case_id, bug_id, source_id, user_id, force_refresh=force_refresh)
        )
    else:
        published = False
        if kafka_producer:
            try:
                published = await publish_triage_request(
                    kafka_producer, case_id, bug_id, source_id, user_id
                )
            except Exception:
                published = False
        if not published:
            from orchestrator.orchestrator import TaskOrchestrator

            orch = TaskOrchestrator()
            asyncio.create_task(
                orch.run(
                    case_id, bug_id, source_id, user_id, force_refresh=force_refresh
                )
            )

    return {
        "case_id": case_id,
        "bug_id": bug_id,
        "source_id": source_id,
        "status": "accepted",
    }


async def get_triage_result(case_id: str, user_id: str) -> dict:
    # Try Redis cache (fast path)
    cached = await get_cached_case_result(case_id)
    if cached:
        # Override the global ID with the user's specific display_id
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            from orchestrator.db.models import AuditLog
            result = await db.execute(
                select(AuditLog.display_id, AuditLog.id)
                .where(AuditLog.case_id == case_id, AuditLog.engineer_id == user_id)
                .limit(1)
            )
            row = result.first()
            if row:
                cached["id"] = row.display_id or row.id
        return cached

    # Fallback: reconstruct from audit_log when cache has expired
    try:
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            from orchestrator.db.models import AuditLog
            # Get the user's specific entry
            result = await db.execute(
                select(AuditLog)
                .where(AuditLog.case_id == case_id, AuditLog.engineer_id == user_id)
                .limit(1)
            )
            entry = result.scalar_one_or_none()
            
            # If not found for this user, fallback to any entry (e.g. shared link)
            if not entry:
                entry = await get_last_triage_by_case_id(db, case_id)
                
        if entry:
            summary = entry.summary or {}
            return {
                "case_id": case_id,
                "id": entry.display_id or entry.id,
                "bug_id": entry.bug_id,
                "source_id": entry.source_id,
                "from_cache": False,
                "context": {
                    "synthesis": {
                        "unified_severity": summary.get("severity"),
                        "confidence": summary.get("confidence"),
                        "root_cause": summary.get("root_cause", ""),
                        "recommended_actions": summary.get("recommended_actions", []),
                        "summary": summary.get("summary", ""),
                        "used_fallback": summary.get("used_fallback", False),
                    },
                    "related_tickets": [],
                    "kb_articles": [],
                    "customer_cases": [],
                },
            }
    except Exception:
        pass

    raise HTTPException(
        status_code=404, detail="Result not found or expired. Please retriage."
    )
