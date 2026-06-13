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
    return await connection_service.list_connections()


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
    )


@router.put("/settings/connections/{source_id}")
async def update_connection(
    source_id: str,
    body: ConnectionUpdate,
    user: User = Depends(get_current_user),
):
    return await connection_service.update_connection(
        source_id, body.dict(exclude_unset=True)
    )


@router.delete("/settings/connections/{source_id}")
async def remove_connection(
    source_id: str,
    user: User = Depends(get_current_user),
):
    return await connection_service.remove_connection(source_id)


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
