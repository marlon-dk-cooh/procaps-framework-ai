"""
Strategy Router
---------------
Factory for retrieving chunking strategies.
"""

from typing import Dict, Type, Optional
from .base import ChunkerStrategy, ChunkingConfig
from .fixed_word import FixedWordChunker
from .recursive import RecursiveChunker
from .markdown import MarkdownChunker
from .semantic import SemanticChunker, IEmbedder
from .character import CharacterChunker

# Registry
_STRATEGIES: Dict[str, Type[ChunkerStrategy]] = {
    "fixed_word": FixedWordChunker,
    "recursive": RecursiveChunker,
    "markdown": MarkdownChunker,
    "semantic": SemanticChunker,
    "character": CharacterChunker,
}


def get_strategy(strategy_name: str) -> ChunkerStrategy:
    """
    Returns an instance of the requested strategy.
    Defaults to 'fixed_word' if not found (or raises error?).
    Let's strict fail or default. Plan says "Default strategy: structural" in config example,
    but here we should just return what's asked.
    """
    strategy_cls = _STRATEGIES.get(strategy_name.lower())
    if not strategy_cls:
        # Fallback to fixed_word for robustness?
        # Or raise ValueError to alert misconfiguration?
        # Let's raise for now to be explicit, but maybe log warning and default in s02.
        raise ValueError(
            f"Unknown chunking strategy: {strategy_name}. Available: {list(_STRATEGIES.keys())}"
        )

    return strategy_cls()


def get_chunker(
    config: ChunkingConfig,
    embedder_model: Optional[IEmbedder] = None,
) -> ChunkerStrategy:
    """
    Factory alineado al flujo de referencia:
    - Recibe `ChunkingConfig`
    - Permite inyección de embedder para estrategia semantic
    - Hace fallback a recursive cuando la estrategia no existe
    """
    strategy_name = (config.strategy or "recursive").lower()

    if strategy_name == "semantic":
        return SemanticChunker(embedder_model=embedder_model)

    strategy_cls = _STRATEGIES.get(strategy_name)
    if strategy_cls:
        return strategy_cls()

    return RecursiveChunker()
