from .base import BaseAgent
from .context_fetch import ContextFetchAgent
from .cross_system_fetch import CrossSystemFetchAgent

__all__ = [
    "BaseAgent",
    "ContextFetchAgent",
    "CrossSystemFetchAgent",
    "EnrichmentAgent",
    "AISynthesisAgent",
]


def __getattr__(name):
    if name == "EnrichmentAgent":
        from .enrichment import EnrichmentAgent

        return EnrichmentAgent
    if name == "AISynthesisAgent":
        from .ai_synthesis import AISynthesisAgent

        return AISynthesisAgent
    raise AttributeError(name)
