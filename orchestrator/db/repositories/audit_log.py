from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from ..models import AuditLog


async def insert_audit_entry(db: AsyncSession, data: dict) -> AuditLog:
    if "engineer_id" in data and data["engineer_id"] and "display_id" not in data:
        result = await db.execute(
            select(func.coalesce(func.max(AuditLog.display_id), 0))
            .where(AuditLog.engineer_id == data["engineer_id"])
        )
        data["display_id"] = result.scalar() + 1
        
    entry = AuditLog(**data)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return entry


async def get_last_triage_for_bug(db: AsyncSession, bug_id: str, engineer_id: str | None = None) -> AuditLog | None:
    query = select(AuditLog).where(AuditLog.bug_id == bug_id, AuditLog.step == "pipeline_complete")
    if engineer_id:
        query = query.where(AuditLog.engineer_id == engineer_id)
    query = query.order_by(desc(AuditLog.created_at)).limit(1)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_metrics_summary(db: AsyncSession, engineer_id: str | None = None) -> dict:
    query = select(func.count(AuditLog.id)).where(AuditLog.step == "pipeline_complete")
    if engineer_id:
        query = query.where(AuditLog.engineer_id == engineer_id)
    total = await db.execute(query)
    total_count = total.scalar() or 0

    return {
        "total_triaged": total_count,
    }


async def get_last_triage_by_case_id(db: AsyncSession, case_id: str) -> AuditLog | None:
    result = await db.execute(
        select(AuditLog)
        .where(AuditLog.case_id == case_id)
        .order_by(desc(AuditLog.created_at))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def list_recent_pipeline_completions(db: AsyncSession, limit: int = 50, engineer_id: str | None = None) -> list[AuditLog]:
    query = select(AuditLog).where(AuditLog.step == "pipeline_complete")
    if engineer_id:
        query = query.where(AuditLog.engineer_id == engineer_id)
    query = query.order_by(desc(AuditLog.created_at)).limit(limit)
    result = await db.execute(query)
    return list(result.scalars().all())
