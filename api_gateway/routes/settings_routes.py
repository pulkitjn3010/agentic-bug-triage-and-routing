from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from urllib.parse import urlparse
from ..auth import get_current_user, User
from orchestrator.db.session import AsyncSessionLocal
from orchestrator.db.repositories.source_registry import (
    get_all_sources,
    get_source_by_id,
    create_source,
    set_source_enabled,
    update_source,
)
from orchestrator.connectors.registry import ConnectorRegistry, SYSTEM_TYPE_TO_CLASS
from ..services import connection_service

router = APIRouter(tags=["settings"])

class ConnectionCreate(BaseModel):
    display_name: str
    system_type: str
    base_url: str
    port: Optional[int] = None
    auth_type: Optional[str] = "bearer_token"
    auth_token: Optional[str] = ""
    project_key: Optional[str] = ""
    ticket_prefix: Optional[str] = ""


class ConnectionUpdate(BaseModel):
    display_name: Optional[str] = None
    base_url: Optional[str] = None
    port: Optional[int] = None
    auth_type: Optional[str] = None
    auth_token: Optional[str] = None
    token: Optional[str] = None
    project_key: Optional[str] = None
    ticket_prefix: Optional[str] = None
    enabled: Optional[bool] = None



@router.get("/settings/connections")
async def list_connections(user: User = Depends(get_current_user)):
    return await connection_service.list_connections(user_id=user.user_id)


@router.post("/settings/connections")
async def add_connection(
    body: ConnectionCreate,
    user: User = Depends(get_current_user),
):
    return await connection_service.create_connection(
        display_name=body.display_name,
        system_type=body.system_type,
        base_url=body.base_url,
        port=body.port,
        auth_type=body.auth_type,
        auth_token=body.auth_token,
        project_key=body.project_key,
        ticket_prefix=body.ticket_prefix,
        user_id=user.user_id,
    )


@router.put("/settings/connections/{source_id}")
async def update_connection(
    source_id: str,
    body: ConnectionUpdate,
    user: User = Depends(get_current_user),
):
    async with AsyncSessionLocal() as db:
        source = await get_source_by_id(db, source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Connection not found")

        updates = {}
        if body.display_name is not None:
            updates["display_name"] = body.display_name
        if body.base_url is not None:
            updates["base_url"] = body.base_url.rstrip("/")
        if body.port is not None:
            updates["port"] = body.port
        if body.auth_type is not None:
            updates["auth_type"] = body.auth_type
        if body.project_key is not None:
            updates["project_key"] = body.project_key
        if body.ticket_prefix is not None:
            updates["ticket_prefix"] = body.ticket_prefix
        if body.enabled is not None:
            updates["enabled"] = body.enabled
            if body.enabled:
                from orchestrator.db.models import utcnow
                updates["created_at"] = utcnow()

        new_token = body.auth_token or body.token
        if new_token:
            env_var = source.auth_secret_ref or ""
            if env_var:
                os.environ[env_var] = new_token

        if source.owner_id is None:
            # It's a global template, create or update a user override
            override_id = f"{user.user_id}-{source_id}"
            override = await get_source_by_id(db, override_id)
            if override:
                await update_source(db, override_id, updates)
                source = await get_source_by_id(db, override_id)
            else:
                from orchestrator.db.repositories.source_registry import SourceRegistry
                from orchestrator.db.models import utcnow
                new_override = SourceRegistry(
                    source_id=override_id,
                    display_name=updates.get("display_name", source.display_name),
                    system_type=source.system_type,
                    base_url=updates.get("base_url", source.base_url),
                    port=updates.get("port", source.port),
                    auth_type=updates.get("auth_type", source.auth_type),
                    auth_secret_ref=source.auth_secret_ref,
                    project_key=updates.get("project_key", source.project_key),
                    ticket_prefix=updates.get("ticket_prefix", source.ticket_prefix),
                    owner_id=user.user_id,
                    enabled=updates.get("enabled", source.enabled),
                    created_at=updates.get("created_at") or source.created_at or utcnow(),
                )
                db.add(new_override)
                await db.commit()
                source = new_override
        elif updates:
            await update_source(db, source_id, updates)
            source = await get_source_by_id(db, source_id)

    ConnectorRegistry.invalidate_cache()
    return {"connection": connection_service._format_source(source), "status": "updated"}


@router.delete("/settings/connections/{source_id}")
async def remove_connection(
    source_id: str,
    user: User = Depends(get_current_user),
):
    async with AsyncSessionLocal() as db:
        source = await get_source_by_id(db, source_id)
        if not source:
            raise HTTPException(status_code=404, detail="Connection not found")
        await set_source_enabled(db, source_id, False, user_id=user.user_id)

    ConnectorRegistry.invalidate_cache()
    return {"status": "disabled", "source_id": source_id}


@router.put("/connections/{source_id}")
async def update_connection_legacy(
    source_id: str,
    payload: dict,
    user: User = Depends(get_current_user),
):
    if user.role not in ("admin", "engineer"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return await connection_service.update_connection_legacy(source_id, payload)


@router.post("/settings/connections/{source_id}/test")
async def test_connection(
    source_id: str,
    user: User = Depends(get_current_user),
):
    return await connection_service.test_connection(source_id)
