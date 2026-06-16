import time
from abc import ABC, abstractmethod
from ..models.ticket import TicketData, ChangeEvent


class BaseConnector(ABC):
    BUG_SOURCE_TYPES = {
        "jira",
        "jira_apache",
        "jira_cloud",
        "github",
        "bugzilla",
    }
    KNOWLEDGE_SOURCE_TYPES = {"confluence", "support_kb"}
    CUSTOMER_SOURCE_TYPES = {"customer_portal"}

    def __init__(
        self,
        source_id: str,
        system_type: str,
        base_url: str,
        project_key: str,
        ticket_prefix: str,
        token: str = "",
    ):
        self.source_id = source_id
        self.system_type = system_type
        self.base_url = (base_url or "").rstrip("/")
        self.project_key = project_key or ""
        self.ticket_prefix = ticket_prefix or ""
        self.token = token

    @property
    def cache_key(self) -> str:
        import hashlib
        raw = f"{self.system_type}:{self.base_url}:{self.project_key}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @property
    def is_bug_source(self) -> bool:
        return self.system_type in self.BUG_SOURCE_TYPES

    @property
    def is_knowledge_source(self) -> bool:
        return self.system_type in self.KNOWLEDGE_SOURCE_TYPES

    @property
    def is_customer_source(self) -> bool:
        return self.system_type in self.CUSTOMER_SOURCE_TYPES

    @abstractmethod
    async def get(self, ticket_id: str) -> TicketData | None:
        pass

    async def get_ticket(self, ticket_id: str) -> TicketData | None:
        return await self.get(ticket_id)

    @abstractmethod
    async def search(self, query: str, max_results: int = 10) -> list[TicketData]:
        pass

    async def search_open_bugs(
        self, status: str = "open", severity: str = "", max_results: int = 100, **kwargs
    ) -> list[TicketData]:
        return await self.search("", max_results=max_results)

    @abstractmethod
    async def get_linked_items(self, ticket_id: str) -> list[dict]:
        pass

    async def get_changelog(self, ticket_id: str, since: str = "") -> list[ChangeEvent]:
        return []

    @abstractmethod
    async def get_lightweight(self, ticket_id: str) -> dict:
        """Fetch only updated_at, severity, status. Returns {} on failure."""

    async def lightweight_status(self, ticket_id: str) -> dict:
        return await self.get_lightweight(ticket_id)

    async def health_check(self) -> dict:
        started = time.perf_counter()
        try:
            await self.search_open_bugs(max_results=1)
            latency_ms = round((time.perf_counter() - started) * 1000, 1)
            return {
                "source_id": self.source_id,
                "system_type": self.system_type,
                "status": "ok",
                "ok": True,
                "latency_ms": latency_ms,
                "error": "",
            }
        except Exception as e:
            latency_ms = round((time.perf_counter() - started) * 1000, 1)
            return {
                "source_id": self.source_id,
                "system_type": self.system_type,
                "status": "error",
                "ok": False,
                "latency_ms": latency_ms,
                "error": str(e)[:300],
            }

    @abstractmethod
    def extract_links(self, raw_payload: dict) -> list[dict]:
        """
        Parse raw API payload and return explicit outbound references.
        Each reference is a dict with keys:
          raw_id, source, relationship, url (optional)
        Returns empty list if not supported.
        """

    def reference_variants(self, ticket_id: str, url: str = "", raw_key: str = "") -> list[str]:
        variants = {v for v in (ticket_id, raw_key, url) if v}
        return sorted(variants)

    def extract_references_from_text(self, text: str) -> list[dict]:
        return []

    def accepts_search_query(self, query: str) -> bool:
        query_l = (query or "").lower()
        if query_l.startswith(("http://", "https://")):
            base = (self.base_url or "").lower()
            return bool(base and base in query_l)
        return True

    def normalize_reference_id(self, ticket_id: str) -> str:
        return str(ticket_id or "").strip()

    def relationship_hint_from_text(self, text: str, pos: int) -> str:
        window = (text or "")[max(0, pos - 80): pos + 80].lower()
        if any(word in window for word in ("blocks", "blocked by", "depends on", "dependency", "caused by", "fixed by", "requires")):
            return "dependency"
        if "duplicate" in window:
            return "duplicate"
        return "direct_reference"
