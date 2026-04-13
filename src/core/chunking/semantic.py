"""
Semantic Chunker
----------------
Reference-aligned semantic strategy using LangChain Experimental SemanticChunker.
"""

from typing import Protocol, List, Optional
import uuid

from ..logging_config import get_logger
from ..ir_models import DocumentIR, Chunk, ChunkMetadata
from .base import ChunkerStrategy, ChunkingConfig, IChunker

logger = get_logger(__name__)


class IEmbedder(Protocol):
    """Minimal embeddings protocol expected by LangChain chunkers."""

    def embed_documents(self, texts: List[str]) -> List[List[float]]: ...

    def embed_query(self, text: str) -> List[float]: ...


class SentenceTransformerEmbedder:
    """
    Adapter that wraps sentence-transformers in a LangChain-compatible interface.
    """

    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer  # pyright: ignore[reportMissingImports]

        self._model = SentenceTransformer(model_name)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        vectors = self._model.encode(texts)
        return (
            vectors.tolist()
            if hasattr(vectors, "tolist")
            else [list(v) for v in vectors]
        )

    def embed_query(self, text: str) -> List[float]:
        vector = self._model.encode(text)
        return vector.tolist() if hasattr(vector, "tolist") else list(vector)


class SemanticChunkerImpl(IChunker):
    """
    Thin wrapper over `langchain_experimental.text_splitter.SemanticChunker`.
    """

    def __init__(
        self,
        embedder_implementation: IEmbedder,
        breakpoint_threshold_type: str,
        breakpoint_threshold_amount: float,
    ):
        from langchain_experimental.text_splitter import (
            SemanticChunker as LangchainSemanticChunker,
        )  # pyright: ignore[reportMissingImports]

        self.splitter = LangchainSemanticChunker(
            embeddings=embedder_implementation,
            breakpoint_threshold_type=breakpoint_threshold_type,
            breakpoint_threshold_amount=breakpoint_threshold_amount,
        )

    def split_text(self, text: str) -> List[str]:
        if not text:
            return []
        docs = self.splitter.create_documents([text])
        return [d.page_content for d in docs if getattr(d, "page_content", "").strip()]


class SemanticChunker(ChunkerStrategy):
    """
    Pipeline wrapper that adapts SemanticChunkerImpl to `ChunkerStrategy`.
    """

    def __init__(self, embedder_model: Optional[IEmbedder] = None):
        # Permite inyección externa (alineado con patrón de referencia).
        self._embedder_model = embedder_model

    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        text = ir.plain_text or ""
        if not text.strip():
            return []

        try:
            extra = config.extra_params or {}
            model_name = (
                config.semantic_embedding_model
                or extra.get("semantic_embedding_model")
                or extra.get("embedding_model")
                or "all-MiniLM-L6-v2"
            )
            threshold_type = (
                config.semantic_threshold_type
                or extra.get("semantic_threshold_type")
                or "percentile"
            )
            threshold_amount = (
                config.semantic_breakpoint_threshold_amount
                if config.semantic_breakpoint_threshold_amount is not None
                else extra.get("semantic_breakpoint_threshold_amount", 95.0)
            )

            embedder = self._embedder_model or SentenceTransformerEmbedder(
                model_name=model_name
            )
            splitter = SemanticChunkerImpl(
                embedder_implementation=embedder,
                breakpoint_threshold_type=threshold_type,
                breakpoint_threshold_amount=float(threshold_amount),
            )
            parts = splitter.split_text(text)
        except Exception as exc:
            logger.warning(
                f"Semantic chunking unavailable ({exc}). Falling back to RecursiveChunker."
            )
            from .recursive import RecursiveChunker

            return RecursiveChunker().chunk(ir, config)

        chunks: List[Chunk] = []
        for i, content in enumerate(parts):
            if not content or not content.strip():
                continue
            chunks.append(
                Chunk(
                    chunk_id=str(uuid.uuid4()),
                    content=content,
                    metadata=ChunkMetadata(
                        source_doc_id=ir.doc_id,
                        chunk_ordinal=i,
                        strategy="semantic",
                        original_metadata={
                            **(ir.metadata or {}),
                            "semantic_threshold_type": threshold_type,
                            "semantic_breakpoint_threshold_amount": float(
                                threshold_amount
                            ),
                            "semantic_embedding_model": model_name,
                            "total_chunks": len(parts),
                        },
                    ),
                )
            )
        return chunks
