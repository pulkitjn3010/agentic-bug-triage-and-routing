from pydantic import BaseModel
from typing import List, Optional


class GroupedBugRow(BaseModel):
    group_id: str
    title: str
    priority: str
    status: str
    primary_source_id: str
    source_ids: List[str] = []
    system_types: List[str] = []
    ticket_ids: List[str] = []
    last_triaged_at: Optional[str] = None
    needs_retriage: bool = False
    changes_count: int = 0
