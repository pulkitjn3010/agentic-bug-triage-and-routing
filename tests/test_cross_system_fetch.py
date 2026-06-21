import asyncio
from types import SimpleNamespace

from orchestrator.agents import cross_system_fetch as cross_module
from orchestrator.agents.cross_system_fetch import CrossSystemFetchAgent


def run(coro):
    return asyncio.run(coro)


class EmptyRegistry:
    @classmethod
    async def get_all_enabled(cls):
        return []


class CacheOnlyCrossSystemFetchAgent(CrossSystemFetchAgent):
    def __init__(self, candidates):
        self.candidates = candidates

    async def _scan_redis_cache(self, keywords):
        return list(self.candidates)

    async def _live_api_search(
        self, primary, primary_source, context, all_connectors=None
    ):
        return [], []


def primary_context():
    return {
        "source_id": "jira-storage",
        "primary_ticket": {
            "ticket_id": "STO-1089",
            "id": "STO-1089",
            "source_id": "jira-storage",
            "system_type": "jira",
            "title": "StorageController NPE during VM provisioning",
            "description": "NullPointerException in allocate",
            "component": "StorageController",
            "comments": [],
        },
    }


def test_cross_system_excludes_primary_ticket(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setattr(cross_module, "ConnectorRegistry", EmptyRegistry)
    agent = CacheOnlyCrossSystemFetchAgent(
        [
            {
                "id": "STO-1089",
                "ticket_id": "STO-1089",
                "title": "StorageController NPE during VM provisioning",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 9,
            },
            {
                "id": "REL-1",
                "ticket_id": "REL-1",
                "title": "StorageController NullPointerException during VM allocation",
                "description": "StorageController raises NullPointerException in allocate during VM provisioning",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 8,
            },
        ]
    )

    context = run(agent.run(primary_context()))

    ids = [item["ticket_id"] for item in context["related_tickets"]]
    assert "STO-1089" not in ids
    assert ids == ["REL-1"]
    assert context["related_issues"]["related_tickets"] == context["related_tickets"]


def test_cross_system_deduplicates_repeated_related_tickets(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setattr(cross_module, "ConnectorRegistry", EmptyRegistry)
    agent = CacheOnlyCrossSystemFetchAgent(
        [
            {
                "id": "REL-1",
                "ticket_id": "REL-1",
                "title": "StorageController NullPointerException during VM allocation",
                "description": "StorageController raises NullPointerException in allocate during VM provisioning",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 8,
            },
            {
                "id": "REL-1",
                "ticket_id": "REL-1",
                "title": "Duplicate cache copy",
                "description": "StorageController raises NullPointerException in allocate during VM provisioning",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 7,
            },
            {
                "id": "REL-2",
                "ticket_id": "REL-2",
                "title": "StorageController NullPointerException while provisioning a VM",
                "description": "StorageController fails in allocate with NullPointerException during VM provisioning",
                "source": "github",
                "source_id": "spark-github",
                "overlap_score": 6,
            },
        ]
    )

    context = run(agent.run(primary_context()))

    ids = [item["ticket_id"] for item in context["related_tickets"]]
    assert ids == ["REL-1", "REL-2"]


def test_panel_2_output_never_includes_primary_ticket(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setattr(cross_module, "ConnectorRegistry", EmptyRegistry)
    context = primary_context()
    agent = CacheOnlyCrossSystemFetchAgent(
        [
            {
                "id": " sto-1089 ",
                "ticket_id": " sto-1089 ",
                "title": "Whitespace/lowercase primary duplicate",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 10,
            },
            {
                "id": "REL-3",
                "ticket_id": "REL-3",
                "title": "StorageController NullPointerException during VM provisioning",
                "description": "StorageController fails in allocate with NullPointerException during VM provisioning",
                "source": "jira-storage",
                "source_id": "jira-storage",
                "overlap_score": 8,
            },
        ]
    )

    context = run(agent.run(context))
    panel_2 = {"related_tickets": context["related_tickets"]}

    primary_id = context["primary_ticket"]["ticket_id"].strip().upper()
    panel_ids = {
        str(item.get("ticket_id") or item.get("id") or "").strip().upper()
        for item in panel_2["related_tickets"]
    }
    assert primary_id not in panel_ids


def test_primary_connector_is_included_in_search_targets():
    agent = CrossSystemFetchAgent()
    connectors = [
        SimpleNamespace(source_id="jira-storage", system_type="jira", cache_key="jira-a"),
        SimpleNamespace(source_id="github-storage", system_type="github", cache_key="github-a"),
        SimpleNamespace(source_id="bugzilla-storage", system_type="bugzilla", cache_key="bz-a"),
    ]

    targets = agent._target_connectors(
        connectors,
        {"source_id": "jira-storage", "system_type": "jira"},
        connectors[0],
    )

    assert [connector.source_id for connector in targets] == [
        "jira-storage",
        "github-storage",
        "bugzilla-storage",
    ]


def test_final_ranking_is_score_only_and_source_neutral():
    agent = CrossSystemFetchAgent()
    primary = {
        "ticket_id": "STO-1089",
        "source_id": "jira-storage",
        "system_type": "jira",
        "backend_key": "jira-a",
    }
    candidates = [
        {
            "ticket_id": "STO-2000",
            "source_id": "jira-storage",
            "system_type": "jira",
            "backend_key": "jira-a",
            "title": "Same-system match",
            "similarity_score": 0.88,
            "relationship_type": "semantic_similarity",
            "similarity_reason": "STO-2000 and STO-1089 share the same concrete allocation failure and code path.",
        },
        {
            "ticket_id": "#20",
            "source_id": "github-storage",
            "system_type": "github",
            "backend_key": "github-a",
            "title": "External match",
            "similarity_score": 0.93,
            "relationship_type": "semantic_similarity",
            "similarity_reason": "GitHub issue 20 and STO-1089 share the same allocation failure and root cause.",
        },
        {
            "ticket_id": "BZ-30",
            "source_id": "bugzilla-storage",
            "system_type": "bugzilla",
            "backend_key": "bz-a",
            "title": "Lower-scored duplicate label",
            "similarity_score": 0.70,
            "relationship_type": "duplicate",
            "similarity_reason": "BZ-30 overlaps with STO-1089 but has less concrete technical evidence.",
        },
    ]

    final = agent._finalize_candidates(candidates, primary)

    assert [item["source_id"] for item in final] == [
        "github-storage",
        "jira-storage",
        "bugzilla-storage",
    ]
    assert all(item["similarity_score"] >= 0.6 for item in final)


def test_same_numeric_id_in_different_backends_is_not_primary_match():
    agent = CrossSystemFetchAgent()
    primary = {
        "ticket_id": "123",
        "source_id": "github-storage",
        "system_type": "github",
        "backend_key": "github-a",
    }
    candidate = {
        "ticket_id": "BZ-123",
        "source_id": "bugzilla-storage",
        "system_type": "bugzilla",
        "backend_key": "bz-a",
    }

    assert agent._is_primary_match(candidate, primary) is False


def test_deterministic_reason_contains_candidate_specific_evidence():
    agent = CrossSystemFetchAgent()
    primary = {
        "ticket_id": "STO-1089",
        "title": "Controller crashes during provisioning",
        "description": "StorageController.allocate() raises NullPointerException while provisioning a VM",
        "component": "StorageController",
    }
    candidate = {
        "ticket_id": "GH-44",
        "title": "Allocation path crashes for new virtual machine",
        "description": "StorageController.allocate() throws NullPointerException during VM provisioning",
        "component": "StorageController",
        "relationship_hint": "semantic_similarity",
    }

    scored = agent._score_one_deterministic(primary, candidate)

    assert scored["similarity_score"] >= 0.6
    assert "GH-44" in scored["similarity_reason"]
    assert "STO-1089" in scored["similarity_reason"]
    assert "NullPointerException" in scored["similarity_reason"]
    assert scored["similarity_reason"].lower() != candidate["title"].lower()


def test_repeated_generic_model_reasons_are_replaced(monkeypatch):
    candidates = [
        {
            "ticket_id": "GH-44",
            "source_id": "github-a",
            "system_type": "github",
            "title": "Allocation crashes for a virtual machine",
            "description": "StorageController.allocate() throws NullPointerException during provisioning",
        },
        {
            "ticket_id": "BZ-45",
            "source_id": "bugzilla-a",
            "system_type": "bugzilla",
            "title": "Provisioning crashes in allocation",
            "description": "StorageController.allocate() raises NullPointerException while provisioning",
        },
    ]
    model_results = {
        "results": [
            {
                "ticket_id": candidate["ticket_id"],
                "source_id": candidate["source_id"],
                "similarity_score": 0.82,
                "relationship_type": "semantic_similarity",
                "similarity_reason": "Both issues share matching technical evidence with the primary issue.",
                "similarity_matching_fields": [
                    "StorageController.allocate()",
                    "NullPointerException",
                ],
                "is_related": True,
            }
            for candidate in candidates
        ]
    }

    class FakeCompletions:
        async def create(self, **kwargs):
            message = SimpleNamespace(content=cross_module.json.dumps(model_results))
            return SimpleNamespace(choices=[SimpleNamespace(message=message)])

    class FakeGroq:
        def __init__(self, api_key):
            self.chat = SimpleNamespace(completions=FakeCompletions())

    monkeypatch.setattr(cross_module, "AsyncGroq", FakeGroq)
    primary = {
        "ticket_id": "STO-1089",
        "source_id": "jira-a",
        "system_type": "jira",
        "title": "Controller crashes during provisioning",
        "description": "StorageController.allocate() raises NullPointerException during provisioning",
    }

    scored = run(
        CrossSystemFetchAgent()._score_with_groq(
            primary, candidates, api_key="test", model="test-model"
        )
    )
    reasons = [item["similarity_reason"] for item in scored]

    assert len(set(reasons)) == 2
    assert "GH-44" in reasons[0]
    assert "BZ-45" in reasons[1]
    assert all("shares matching technical evidence" not in reason.lower() for reason in reasons)


def test_score_normalization_rejects_unknown_scales_and_non_finite_values():
    agent = CrossSystemFetchAgent()

    assert agent._coerce_score(0.82) == 0.82
    assert agent._coerce_score(10) == 0.0
    assert agent._coerce_score(82) == 0.0
    assert agent._coerce_score(float("nan")) == 0.0
