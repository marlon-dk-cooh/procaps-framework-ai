"""
Character chunker strategy.

Provides a lightweight wrapper around LangChain's CharacterTextSplitter
with the project-specific parameter names: size, overlap, and separator.
"""

from typing import List
import uuid

from ..ir_models import DocumentIR, Chunk, ChunkMetadata
from .base import ChunkerStrategy, ChunkingConfig


class CharacterChunker(ChunkerStrategy):
    """
    Chunking por caracteres usando `langchain_text_splitters.CharacterTextSplitter`.

    Parámetros en `ChunkingConfig`:
    - size (int): tamaño objetivo por chunk (caracteres)
    - overlap (int): overlap (caracteres)
    - separator (str): separador preferido
    - is_separator_regex (bool): se fija en False por diseño

    Fallbacks:
    - size -> chunk_size
    - overlap -> chunk_overlap
    """

    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        text = ir.plain_text or ""
        if not text.strip():
            return []

        size = config.size or config.chunk_size
        overlap = config.overlap if config.overlap is not None else config.chunk_overlap
        separator = config.separator or "\n\n"
        is_separator_regex = config.is_separator_regex

        from langchain_text_splitters import CharacterTextSplitter  # pyright: ignore[reportMissingImports]

        splitter = CharacterTextSplitter(
            separator=separator,
            chunk_size=size,
            chunk_overlap=overlap,
            is_separator_regex=is_separator_regex,
        )

        parts = splitter.split_text(text)
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
                        strategy="character",
                        original_metadata=ir.metadata,
                    ),
                )
            )
        return chunks
