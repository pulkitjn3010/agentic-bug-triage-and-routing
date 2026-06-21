import asyncio
import dataclasses
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from ..auth import get_current_user, User
from ..config import REDIS_TTL_BUGLIST_SECONDS
from orchestrator.connectors.registry import ConnectorRegistry
from orchestrator.redis_client import get_cached_buglist, cache_buglist
from orchestrator.utils.url_utils import sanitize_bug_url
from orchestrator.db.session import AsyncSessionLocal
from orchestrator.db.models import (
    AuditLog,
    SystemGroupRegistry,
    BugGroupMapping,
)
from orchestrator.db.repositories.audit_log import (
    get_last_triage_for_bug,
    get_metrics_summary,
    list_recent_pipeline_completions,
)
from ..services import bug_service, case_service, metrics_service

router = APIRouter(tags=["cases"])
SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "Unknown": 4}
_BUG_SOURCE_TYPES = {"github", "jira", "jira_apache", "bugzilla"}


@router.get("/debug/confluence-test")
async def debug_confluence(q: str = "NormalizeCTEIds"):
    import asyncio
    from orchestrator.connectors.registry import ConnectorRegistry

    connectors = await ConnectorRegistry.get_all_enabled()
    conf = next((c for c in connectors if c.system_type == "confluence"), None)

    if not conf:
        return {"error": "No confluence connector found"}

    try:
        results = await asyncio.wait_for(conf.search(q, max_results=5), timeout=15.0)
        return {
            "connector": conf.source_id,
            "base_url": conf.base_url,
            "query": q,
            "results_count": len(results),
            "titles": [r.title for r in results],
            "urls": [r.url for r in results],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/sources")
async def debug_sources():
    from orchestrator.db.session import AsyncSessionLocal
    from orchestrator.db.repositories.source_registry import get_all_sources
    from orchestrator.connectors.registry import (
        ConnectorRegistry,
        load_connectors_from_db,
    )
    import os

    async with AsyncSessionLocal() as db:
        sources = await get_all_sources(db)

    connectors = await load_connectors_from_db()

    return {
        "db_sources": [
            {
                "source_id": s.source_id,
                "system_type": s.system_type,
                "enabled": s.enabled,
                "auth_secret_ref": s.auth_secret_ref,
                "token_present": bool(os.environ.get(s.auth_secret_ref or "", "")),
                "project_key": s.project_key,
            }
            for s in sources
        ],
        "connectors_loaded": len(connectors),
        "connector_ids": [c.source_id for c in connectors],
    }


def get_bug_score(severity: str, updated_at: str) -> float:
    sev_val = {
        "P0": 4, "P1": 3, "P2": 2, "P3": 1
    }.get(severity or "Unknown", 0)
    ts = 0.0
    if updated_at:
        try:
            s = str(updated_at).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            if len(s) > 5 and s[-5] in ('+', '-'):
                s = s[:-2] + ":" + s[-2:]
            dt = datetime.fromisoformat(s)
            ts = dt.timestamp()
        except Exception:
            pass
    return sev_val * 10 + (ts / 2000000000.0)


def _normalize_list_bug(ticket, connector) -> dict:
    if dataclasses.is_dataclass(ticket):
        data = dataclasses.asdict(ticket)
    elif isinstance(ticket, dict):
        data = dict(ticket)
    else:
        data = {
            "ticket_id": getattr(ticket, "ticket_id", ""),
            "title": getattr(ticket, "title", ""),
            "severity": getattr(ticket, "severity", ""),
            "status": getattr(ticket, "status", ""),
            "assignee": getattr(ticket, "assignee", ""),
            "updated_at": getattr(ticket, "updated_at", ""),
            "url": getattr(ticket, "url", ""),
        }

    ticket_id = data.get("ticket_id") or data.get("id") or ""
    system_type = data.get("system_type") or connector.system_type
    source_id = data.get("source_id") or connector.source_id
    base_url = getattr(connector, "base_url", "")
    return {
        "ticket_id": ticket_id,
        "source": system_type,
        "source_id": source_id,
        "system_type": system_type,
        "source_display_name": getattr(
            connector, "display_name", connector.source_id),
        "project_key": getattr(connector, "project_key", ""),
        "title": data.get("title", ""),
        "severity": data.get("severity") or "Unknown",
        "status": data.get("status") or "open",
        "assignee": data.get("assignee", ""),
        "updated_at": data.get("updated_at", ""),
        "url": sanitize_bug_url(
            url=data.get("url", ""),
            system_type=system_type,
            bug_id=ticket_id,
            base_url=base_url,
        ),
    }


_FETCH_LOCKS = {}

async def _fetch_buglist_for_connector(
        connector,
        status: str,
        severity: str,
        max_results: int) -> dict:
    
    lock_key = f"{connector.cache_key}:{status}:{severity}"
    if lock_key not in _FETCH_LOCKS:
        _FETCH_LOCKS[lock_key] = asyncio.Lock()

    async with _FETCH_LOCKS[lock_key]:
        # Check cache inside the lock to prevent stampede
        cached = await get_cached_buglist(
            connector.cache_key, status, severity)
        if cached is not None:
            return {
                "source_id": connector.source_id,
                "bugs": cached,
                "cache_status": "hit",
                "error": None,
            }

        try:
            tickets = await asyncio.wait_for(
                connector.search_open_bugs(
                    status=status,
                    severity=severity,
                    max_results=max_results,
                ),
                timeout=90.0,
            )
            bugs = [
                _normalize_list_bug(ticket, connector)
                for ticket in (tickets or [])
            ]
            await cache_buglist(
                connector.cache_key, status, severity, bugs,
                ttl=REDIS_TTL_BUGLIST_SECONDS)
            return {
                "source_id": connector.source_id,
                "bugs": bugs,
                "cache_status": "miss",
                "error": None,
            }
        except Exception as e:
            return {
                "source_id": connector.source_id,
                "bugs": [],
                "cache_status": "error",
                "error": {
                    "source_id": connector.source_id,
                    "system_type": connector.system_type,
                    "message": str(e)[:120],
                },
            }


async def _background_fetch_connector(connectors: list) -> None:
    if not connectors:
        return
    
    primary_connector = connectors[0]
    is_shared = len(connectors) > 1
    shared_text = " (shared cache)" if is_shared else ""

    try:
        tickets = await asyncio.wait_for(
            primary_connector.search_open_bugs(
                status="open",
                severity="",
                max_results=300,
            ),
            timeout=90.0,
        )

        if tickets:
            sanitized_data = [
                _normalize_list_bug(ticket, primary_connector)
                for ticket in tickets
            ]

            await cache_buglist(
                primary_connector.cache_key, "open", "",
                sanitized_data, ttl=REDIS_TTL_BUGLIST_SECONDS)

            for c in connectors:
                print(f"[BugList] {c.source_id}: "
                      f"{len(sanitized_data)} bugs cached{shared_text}")

    except Exception as e:
        for c in connectors:
            print(f"[BugList] {c.source_id} "
                  f"background fetch error{shared_text}: {e}")


# ── GET /bugs ─────────────────────────────────────────────────────


@router.get("/bugs")
async def get_bugs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query(""),
    severity: str = Query(""),
    source: str = Query(""),
    status: str = Query(""),
    project: str = Query(""),
    sort_field: str = Query("severity"),
    sort_order: str = Query("desc"),
    user: User = Depends(get_current_user),
):
    return await bug_service.get_bugs(
        page=page,
        page_size=page_size,
        search=search,
        severity=severity,
        source=source,
        status=status,
        project=project,
        sort_field=sort_field,
        sort_order=sort_order,
        user_id=user.user_id,
    )


@router.post("/bugs/warm")
async def warm_bug_cache(user: User = Depends(get_current_user)):
    connectors = await ConnectorRegistry.get_all_enabled()
    excluded = {"confluence", "customer_portal"}

    # Group by cache_key so connectors sharing a backend fetch once
    grouped_connectors = {}
    for c in connectors:
        if c.system_type not in excluded:
            grouped_connectors.setdefault(c.cache_key, []).append(c)

    for group in grouped_connectors.values():
        asyncio.create_task(_background_fetch_connector(group))

    return {
        "status": "warming",
        "connectors": len(connectors),
        "message": (
            f"Cache warming started for {len(connectors)} " f"connectors in background"
        ),
    }


@router.post("/bugs/refresh")
async def refresh_bugs(user: User = Depends(get_current_user)):
    from orchestrator.redis_client import purge_buglist_cache

    cleared = await purge_buglist_cache()
    return {
        "cleared_keys": cleared,
        "message": ("Bug list cache cleared. " "Next GET /bugs will fetch fresh data."),
    }


@router.get("/bugs/{bug_id}/status")
async def get_bug_status(
    bug_id: str,
    user: User = Depends(get_current_user),
):
    return await bug_service.get_bug_status(bug_id, user_id=user.user_id)


@router.get("/metrics")
async def get_metrics(user: User = Depends(get_current_user)):
    return await metrics_service.get_metrics(user_id=user.user_id)


@router.get("/history/triage")
async def get_triage_history(
    limit: int = Query(50, ge=1, le=200),
    user: User = Depends(get_current_user),
):
    async with AsyncSessionLocal() as db:
        entries = await list_recent_pipeline_completions(
            db, limit=limit, engineer_id=user.user_id)

    results = []
    for e in entries:
        summary = e.summary or {}
        results.append({
            "id":              e.display_id or e.id,
            "case_id":         e.case_id or "",
            "bug_id":          e.bug_id,
            "source_id":       e.source_id or "",
            "engineer_id":     e.engineer_id or "",
            "severity":        (
                summary.get("severity")
                or summary.get(
                    "unified_severity", "Unknown")),
            "confidence":      summary.get("confidence", 0),
            "root_cause":      (
                summary.get("root_cause") or "")[:120],
            "duration_ms":     e.duration_ms or 0,
            "systems_queried": e.systems_queried or [],
            "triaged_at":      (
                e.created_at.isoformat()
                if e.created_at else None),
        })
    return results


@router.get("/cases/{case_id}")
async def get_case_result(
    case_id: str,
    user: User = Depends(get_current_user),
):
    return await case_service.get_case_result(case_id)


def _context_from_panels(panels: list[dict]) -> dict:
    ctx = {}
    for event in panels or []:
        panel = event.get("panel")
        data = event.get("data") or {}
        if panel == "bug_context":
            ctx.update(
                {
                    "bug_context": data.get("bug_context") or {},
                    "primary_ticket": data.get("primary_ticket"),
                    "components": data.get("components") or [],
                    "customer_cases": data.get("customer_cases") or [],
                    "customer_signals": data.get("customer_signals") or [],
                    "source_references": data.get("source_references") or [],
                }
            )
        elif panel == "related_issues":
            ctx["related_tickets"] = data.get("related_tickets") or []
            ctx["sources_queried"] = data.get("sources_queried") or []
        elif panel in {"linked_context", "knowledge_base"}:
            ctx["kb_articles"] = data.get("kb_articles") or []
            ctx["kb_reasoning"] = data.get("kb_reasoning") or ""
            if data.get("customer_cases"):
                ctx["customer_cases"] = data.get("customer_cases")
        elif panel == "ai_summary":
            ctx["synthesis"] = data.get("synthesis") or {}
            ctx["errors"] = data.get("errors") or {}
        elif event.get("type") == "pipeline_done":
            ctx["pipeline_done"] = event
    return ctx
