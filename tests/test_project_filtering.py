import pytest
from api_gateway.services.bug_service import _normalize_list_bug, get_bugs
from orchestrator.connectors.registry import ConnectorRegistry

def test_normalize_list_bug_includes_project_key():
    class FakeConnector:
        source_id = "test-source"
        system_type = "github"
        display_name = "Test Source"
        project_key = "TEST-PROJECT"
        base_url = "https://github.com/test/repo"

    ticket = {
        "ticket_id": "123",
        "title": "Bug Title",
        "severity": "P1",
        "status": "open",
        "url": "https://github.com/test/repo/issues/123"
    }

    normalized = _normalize_list_bug(ticket, FakeConnector())
    assert normalized["project_key"] == "TEST-PROJECT"
    assert normalized["ticket_id"] == "123"


@pytest.mark.asyncio
async def test_get_bugs_filters_by_project(monkeypatch):
    class FakeConnector:
        source_id = "source-1"
        system_type = "github"
        project_key = "PROJ-A"
        display_name = "Source 1"
        base_url = "https://github.com/proj-a"

        async def search_open_bugs(self, status, severity, max_results):
            return [
                {
                    "ticket_id": "1",
                    "title": "Bug in Proj A",
                    "severity": "P1",
                    "status": "open",
                    "url": "https://github.com/proj-a/issues/1"
                }
            ]

    class FakeConnector2:
        source_id = "source-2"
        system_type = "github"
        project_key = "PROJ-B"
        display_name = "Source 2"
        base_url = "https://github.com/proj-b"

        async def search_open_bugs(self, status, severity, max_results):
            return [
                {
                    "ticket_id": "2",
                    "title": "Bug in Proj B",
                    "severity": "P2",
                    "status": "open",
                    "url": "https://github.com/proj-b/issues/2"
                }
            ]

    async def mock_get_all_enabled():
        return [FakeConnector(), FakeConnector2()]

    monkeypatch.setattr(ConnectorRegistry, "get_all_enabled", mock_get_all_enabled)

    # Mock Redis client functions
    monkeypatch.setattr("orchestrator.redis_client.get_cached_buglist", lambda *a, **kw: None)
    monkeypatch.setattr("orchestrator.redis_client.cache_buglist", lambda *a, **kw: None)

    # Mock assemble_grouped_bug_list to bypass database queries
    import api_gateway.services.bug_service as bug_service
    async def mock_assemble(raw_bugs, db):
        for b in raw_bugs:
            b["type"] = "standalone"
            b["is_triaged"] = False
            b["triage_info"] = None
        return {"ungrouped": raw_bugs, "groups": []}
    monkeypatch.setattr(bug_service, "assemble_grouped_bug_list", mock_assemble)

    # Test filtering for PROJ-A
    res = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="",
        status="",
        sort_field="severity",
        sort_order="desc",
        project="PROJ-A"
    )
    bugs = res["bugs"]
    assert len(bugs) == 1
    assert bugs[0]["ticket_id"] == "1"
    assert bugs[0]["project_key"] == "PROJ-A"

    # Test filtering for PROJ-B
    res2 = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="",
        status="",
        sort_field="severity",
        sort_order="desc",
        project="PROJ-B"
    )
    bugs2 = res2["bugs"]
    assert len(bugs2) == 1
    assert bugs2[0]["ticket_id"] == "2"
    assert bugs2[0]["project_key"] == "PROJ-B"

    # Test filtering with non-existent project (should return empty)
    res3 = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="",
        status="",
        sort_field="severity",
        sort_order="desc",
        project="NON-EXISTENT"
    )
    assert len(res3["bugs"]) == 0
