"""
Chunker Strategy Interface
--------------------------
Base class for all chunking strategies.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Protocol, Tuple
from dataclasses import dataclass, field

from ..ir_models import DocumentIR, Chunk


class IChunker(Protocol):
    """
    Protocolo simple de particionamiento por texto plano.
    Se conserva para alinear estrategias tipo `split_text`.
    """

    def split_text(self, text: str) -> List[str]: ...


@dataclass
class ChunkingConfig:
    """
    Configuration parameters passed to chunkers.

    Organizado por estrategia para que `config.yaml` sea explícito y trazable.
    """

    strategy: str

    # ---------------------------------------------------------------------
    # Parámetros base compartidos (fallback para múltiples estrategias)
    # ---------------------------------------------------------------------
    chunk_size: int = 1000
    chunk_overlap: int = 200
    min_chunk_size: int = 50

    # ---------------------------------------------------------------------
    # fixed_word strategy
    # ---------------------------------------------------------------------
    chunk_size_words: Optional[int] = None
    chunk_overlap_words: int = 0
    min_words: Optional[int] = None
    strip_whitespace: bool = True
    normalize_spaces: bool = True

    # ---------------------------------------------------------------------
    # character strategy (CharacterTextSplitter)
    # ---------------------------------------------------------------------
    size: Optional[int] = None
    overlap: Optional[int] = None
    separator: Optional[str] = None
    is_separator_regex: bool = False

    # ---------------------------------------------------------------------
    # recursive strategy (RecursiveCharacterTextSplitter)
    # ---------------------------------------------------------------------
    separators: Optional[List[str]] = None
    keep_separator: bool = True
    recursive_is_separator_regex: bool = False

    # ---------------------------------------------------------------------
    # markdown strategy
    # ---------------------------------------------------------------------
    markdown_headers: Optional[List[Tuple[str, str]]] = None

    # ---------------------------------------------------------------------
    # semantic strategy
    # ---------------------------------------------------------------------
    semantic_threshold_type: str = "percentile"
    semantic_breakpoint_threshold_amount: float = 95.0
    semantic_embedding_model: Optional[str] = None

    # Extensión libre para compatibilidad hacia atrás
    extra_params: Dict[str, Any] = field(default_factory=dict)


class ChunkerStrategy(ABC):
    """
    Abstract Base Class for chunking strategies.
    Each strategy must implement the `chunk` method.
    """

    @abstractmethod
    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        """
        Splits a DocumentIR into a list of Chunks based on the provided configuration.

        Args:
            ir: The intermediate representation of the document.
            config: Configuration parameters for chunking.

        Returns:
            List[Chunk]: The resulting chunks.
        """
        pass
