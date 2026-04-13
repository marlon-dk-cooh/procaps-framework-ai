from typing import List
import uuid

from langchain_text_splitters import RecursiveCharacterTextSplitter

from ..ir_models import DocumentIR, Chunk, ChunkMetadata
from .base import ChunkerStrategy, ChunkingConfig


class RecursiveChunker(ChunkerStrategy):
    """
    Wrapper sobre LangChain RecursiveCharacterTextSplitter
    para que el comportamiento coincida con la implementación anterior.
    """

    DEFAULT_SEPARATORS = ["\n\n", "\n", " ", ""]

    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        text = ir.plain_text or ""
        if not text.strip():
            return []

        extra_params = config.extra_params or {}
        separators = (
            config.separators
            or extra_params.get("separators")
            or self.DEFAULT_SEPARATORS
        )
        keep_separator = extra_params.get("keep_separator", config.keep_separator)
        is_separator_regex = extra_params.get(
            "is_separator_regex",
            config.recursive_is_separator_regex,
        )

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
            separators=separators,
            keep_separator=keep_separator,
            is_separator_regex=is_separator_regex,
        )

        final_chunks = [c for c in splitter.split_text(text) if c and c.strip()]

        total_chunks = len(final_chunks)
        chunks: List[Chunk] = []

        # Búsqueda aproximada de offsets.
        # Con overlap, el siguiente chunk puede empezar antes del end_index anterior.
        cursor = 0

        for i, content in enumerate(final_chunks):
            search_start = max(0, cursor - config.chunk_overlap - 50)
            start_index = text.find(content, search_start)

            if start_index == -1:
                start_index = text.find(content)

            end_index = start_index + len(content) if start_index != -1 else -1

            if end_index != -1:
                cursor = end_index

            chunks.append(
                Chunk(
                    chunk_id=str(uuid.uuid4()),
                    content=content,
                    metadata=ChunkMetadata(
                        source_doc_id=ir.doc_id,
                        chunk_ordinal=i,
                        strategy="recursive",
                        original_metadata={
                            **(ir.metadata or {}),
                            "chunk_size_chars": len(content),
                            "total_chunks": total_chunks,
                            "start_index": start_index,
                            "end_index": end_index,
                            "separators_used": separators,
                            "keep_separator": keep_separator,
                            "is_separator_regex": is_separator_regex,
                        },
                    ),
                )
            )

        return chunks
