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
        cache_key = "fake-1"

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
        cache_key = "fake-2"

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

    async def mock_get_all_enabled(user_id=None):
        return [FakeConnector(), FakeConnector2()]

    monkeypatch.setattr(ConnectorRegistry, "get_all_enabled", mock_get_all_enabled)

    # Mock Redis client functions
    monkeypatch.setattr("orchestrator.redis_client.get_cached_buglist", lambda *a, **kw: None)
    monkeypatch.setattr("orchestrator.redis_client.cache_buglist", lambda *a, **kw: None)

    # Mock assemble_grouped_bug_list to bypass database queries
    import api_gateway.services.bug_service as bug_service
    async def mock_assemble(raw_bugs, db, user_id=None):
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


@pytest.mark.asyncio
async def test_get_bugs_filters_by_jira_source(monkeypatch):
    class FakeJiraConnector:
        source_id = "jira-source"
        system_type = "jira_apache"
        project_key = "JIRA-PROJ"
        display_name = "Jira Source"
        base_url = "https://jira.apache.org"
        cache_key = "fake-jira"

        async def search_open_bugs(self, status, severity, max_results):
            return [
                {
                    "ticket_id": "100",
                    "title": "Jira Bug",
                    "severity": "P2",
                    "status": "open",
                    "url": "https://jira.apache.org/issues/100"
                }
            ]

    class FakeGithubConnector:
        source_id = "gh-source"
        system_type = "github"
        project_key = "GH-PROJ"
        display_name = "Github Source"
        base_url = "https://github.com"
        cache_key = "fake-gh"

        async def search_open_bugs(self, status, severity, max_results):
            return [
                {
                    "ticket_id": "200",
                    "title": "Github Bug",
                    "severity": "P1",
                    "status": "open",
                    "url": "https://github.com/issues/200"
                }
            ]

    async def mock_get_all_enabled(user_id=None):
        return [FakeJiraConnector(), FakeGithubConnector()]

    monkeypatch.setattr(ConnectorRegistry, "get_all_enabled", mock_get_all_enabled)

    # Mock Redis client functions
    monkeypatch.setattr("orchestrator.redis_client.get_cached_buglist", lambda *a, **kw: None)
    monkeypatch.setattr("orchestrator.redis_client.cache_buglist", lambda *a, **kw: None)

    # Mock assemble_grouped_bug_list to bypass database queries
    import api_gateway.services.bug_service as bug_service
    async def mock_assemble(raw_bugs, db, user_id=None):
        for b in raw_bugs:
            b["type"] = "standalone"
            b["is_triaged"] = False
            b["triage_info"] = None
        return {"ungrouped": raw_bugs, "groups": []}
    monkeypatch.setattr(bug_service, "assemble_grouped_bug_list", mock_assemble)

    # Test filtering with source="jira"
    res = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="jira",
        status="",
        sort_field="severity",
        sort_order="desc",
    )
    bugs = res["bugs"]
    assert len(bugs) == 1
    assert bugs[0]["ticket_id"] == "100"
    assert bugs[0]["system_type"] == "jira_apache"


@pytest.mark.asyncio
async def test_get_bugs_deduplicates_global_and_user_backend_aliases(monkeypatch):
    import api_gateway.services.bug_service as bug_service

    calls = []

    class FakeConnector:
        system_type = "jira"
        project_key = "SPARK"
        display_name = "Apache Spark JIRA"
        base_url = "https://issues.apache.org/jira"
        cache_key = "shared-jira-backend"

        def __init__(self, source_id, owner_id):
            self.source_id = source_id
            self.owner_id = owner_id

        async def search_open_bugs(self, status, severity, max_results):
            calls.append(self.source_id)
            return [{
                "ticket_id": "SPARK-1",
                "title": "History server failure",
                "severity": "P1",
                "status": "open",
            }]

    connectors = [
        FakeConnector("apache-spark-jira", None),
        FakeConnector("engineer@example.com-apache-spark-jira", "engineer@example.com"),
    ]

    async def get_connectors(user_id=None):
        return connectors

    async def no_cache(*args, **kwargs):
        return None

    async def ignore_cache_write(*args, **kwargs):
        return None

    async def assemble(raw_bugs, db, user_id=None):
        return {"ungrouped": raw_bugs, "groups": []}

    class EmptyResult:
        def all(self):
            return []

    class FakeDb:
        async def execute(self, *args, **kwargs):
            return EmptyResult()

    class FakeSession:
        async def __aenter__(self):
            return FakeDb()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ConnectorRegistry, "get_all_enabled", get_connectors)
    monkeypatch.setattr(bug_service, "get_cached_buglist", no_cache)
    monkeypatch.setattr(bug_service, "cache_buglist", ignore_cache_write)
    monkeypatch.setattr(bug_service, "assemble_grouped_bug_list", assemble)
    monkeypatch.setattr(bug_service, "AsyncSessionLocal", lambda: FakeSession())

    result = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="",
        status="",
        sort_field="severity",
        sort_order="desc",
        user_id="engineer@example.com",
    )

    assert calls == ["engineer@example.com-apache-spark-jira"]
    assert result["total"] == 1
    assert [bug["ticket_id"] for bug in result["bugs"]] == ["SPARK-1"]


@pytest.mark.asyncio
async def test_get_bugs_keeps_same_numeric_id_from_different_backends(monkeypatch):
    import api_gateway.services.bug_service as bug_service

    class FakeConnector:
        display_name = "Source"
        project_key = "PROJECT"
        base_url = "https://example.test"
        owner_id = "engineer@example.com"

        def __init__(self, source_id, system_type, cache_key):
            self.source_id = source_id
            self.system_type = system_type
            self.cache_key = cache_key

        async def search_open_bugs(self, status, severity, max_results):
            return [{"ticket_id": "123", "title": self.source_id, "status": "open"}]

    connectors = [
        FakeConnector("github-source", "github", "github-backend"),
        FakeConnector("bugzilla-source", "bugzilla", "bugzilla-backend"),
    ]

    async def get_connectors(user_id=None):
        return connectors

    async def no_cache(*args, **kwargs):
        return None

    async def assemble(raw_bugs, db, user_id=None):
        return {"ungrouped": raw_bugs, "groups": []}

    class EmptyResult:
        def all(self):
            return []

    class FakeDb:
        async def execute(self, *args, **kwargs):
            return EmptyResult()

    class FakeSession:
        async def __aenter__(self):
            return FakeDb()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ConnectorRegistry, "get_all_enabled", get_connectors)
    monkeypatch.setattr(bug_service, "get_cached_buglist", no_cache)
    monkeypatch.setattr(bug_service, "cache_buglist", no_cache)
    monkeypatch.setattr(bug_service, "assemble_grouped_bug_list", assemble)
    monkeypatch.setattr(bug_service, "AsyncSessionLocal", lambda: FakeSession())

    result = await get_bugs(
        page=1,
        page_size=10,
        search="",
        severity="",
        source="",
        status="",
        sort_field="severity",
        sort_order="desc",
        user_id="engineer@example.com",
    )

    assert result["total"] == 2
    assert {bug["source_id"] for bug in result["bugs"]} == {
        "github-source",
        "bugzilla-source",
    }
