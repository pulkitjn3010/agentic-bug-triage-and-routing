from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from ..models import SourceRegistry


async def get_all_sources(db: AsyncSession, user_id: str | None = None) -> list[SourceRegistry]:
    if not user_id:
        result = await db.execute(select(SourceRegistry).where(SourceRegistry.owner_id == None))
        return list(result.scalars().all())

    # Get global templates
    global_result = await db.execute(select(SourceRegistry).where(SourceRegistry.owner_id == None))
    global_sources = {s.source_id: s for s in global_result.scalars().all()}

    # Get user overrides
    user_result = await db.execute(select(SourceRegistry).where(SourceRegistry.owner_id == user_id))
    user_rows = list(user_result.scalars().all())
    user_sources = {s.source_id.replace(f"{user_id}-", "", 1): s for s in user_rows if s.source_id.startswith(f"{user_id}-")}
    # Also include custom ones that user created which don't map to a global template
    custom_sources = [s for s in user_rows if not s.source_id.startswith(f"{user_id}-") or s.source_id.replace(f"{user_id}-", "", 1) not in global_sources]

    merged_sources = []
    for g_id, g_source in global_sources.items():
        if g_id in user_sources:
            # Override enabled status from user
            g_source.enabled = user_sources[g_id].enabled
        else:
            g_source.enabled = False
        merged_sources.append(g_source)

    merged_sources.extend(custom_sources)
    return merged_sources


async def get_enabled_sources(db: AsyncSession, user_id: str | None = None) -> list[SourceRegistry]:
    if user_id:
        all_sources = await get_all_sources(db, user_id=user_id)
        return [s for s in all_sources if s.enabled]
        
    query = select(SourceRegistry).where(SourceRegistry.enabled == True)
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_source_by_id(db: AsyncSession, source_id: str) -> SourceRegistry | None:
    result = await db.execute(
        select(SourceRegistry).where(SourceRegistry.source_id == source_id)
    )
    return result.scalar_one_or_none()


async def create_source(db: AsyncSession, data: dict) -> SourceRegistry:
    source = SourceRegistry(**data)
    db.add(source)
    await db.commit()
    await db.refresh(source)
    return source


async def update_source(
    db: AsyncSession, source_id: str, data: dict
) -> SourceRegistry | None:
    await db.execute(
        update(SourceRegistry)
        .where(SourceRegistry.source_id == source_id)
        .values(**data)
    )
    await db.commit()
    return await get_source_by_id(db, source_id)


async def set_source_enabled(db: AsyncSession, source_id: str, enabled: bool, user_id: str | None = None) -> None:
    if not user_id:
        await db.execute(
            update(SourceRegistry).where(SourceRegistry.source_id == source_id).values(enabled=enabled)
        )
        await db.commit()
        return

    # Check if it's a global source being toggled
    global_source = await get_source_by_id(db, source_id)
    
    if global_source and global_source.owner_id is None:
        # Create or update user override
        override_id = f"{user_id}-{source_id}"
        override = await get_source_by_id(db, override_id)
        if override:
            await db.execute(update(SourceRegistry).where(SourceRegistry.source_id == override_id).values(enabled=enabled))
        else:
            new_override = SourceRegistry(
                source_id=override_id,
                display_name=global_source.display_name,
                system_type=global_source.system_type,
                base_url=global_source.base_url,
                port=global_source.port,
                auth_type=global_source.auth_type,
                auth_secret_ref=global_source.auth_secret_ref,
                project_key=global_source.project_key,
                ticket_prefix=global_source.ticket_prefix,
                owner_id=user_id,
                enabled=enabled,
            )
            db.add(new_override)
        await db.commit()
    else:
        await db.execute(
            update(SourceRegistry).where(SourceRegistry.source_id == source_id).values(enabled=enabled)
        )
        await db.commit()
