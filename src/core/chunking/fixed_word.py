from typing import List, Tuple
import re
import uuid

from ..ir_models import DocumentIR, Chunk, ChunkMetadata
from .base import ChunkerStrategy, ChunkingConfig


class FixedWordChunker(ChunkerStrategy):
    """
    Divide el texto por número fijo de palabras usando ventana deslizante.
    Alineado con la documentación:
    - chunk_size_words
    - chunk_overlap_words
    - min_words
    - strip_whitespace
    - normalize_spaces
    """

    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        text = ir.plain_text or ""
        if not text.strip():
            return []

        # Prefer doc-aligned params; fallback to generic ones.
        chunk_size_words = config.chunk_size_words or config.chunk_size
        chunk_overlap_words = (
            config.chunk_overlap_words
            if config.chunk_size_words is not None
            else config.chunk_overlap
        )
        min_words = config.min_words
        strip_whitespace = config.strip_whitespace
        normalize_spaces = config.normalize_spaces

        if chunk_size_words is None or chunk_size_words <= 0:
            raise ValueError("fixed_word requiere 'chunk_size_words' > 0.")

        if chunk_overlap_words < 0:
            raise ValueError("'chunk_overlap_words' no puede ser negativo.")

        if chunk_overlap_words >= chunk_size_words:
            raise ValueError(
                "'chunk_overlap_words' debe ser menor que 'chunk_size_words'."
            )

        if min_words is not None and min_words > chunk_size_words:
            raise ValueError("'min_words' no puede ser mayor que 'chunk_size_words'.")

        # Encuentra palabras preservando posición en el texto original
        words_with_positions = [
            {
                "word": match.group(),
                "start": match.start(),
                "end": match.end(),
            }
            for match in re.finditer(r"\S+", text)
        ]

        if not words_with_positions:
            return []

        step = chunk_size_words - chunk_overlap_words
        ranges: List[Tuple[int, int]] = []

        # Construcción de ventanas
        for start_idx in range(0, len(words_with_positions), step):
            end_idx = min(start_idx + chunk_size_words, len(words_with_positions))
            ranges.append((start_idx, end_idx))

            if end_idx >= len(words_with_positions):
                break

        # Regla para evitar chunk final demasiado pequeño
        if (
            min_words is not None
            and len(ranges) >= 2
            and (ranges[-1][1] - ranges[-1][0]) < min_words
        ):
            prev_start, prev_end = ranges[-2]
            _, last_end = ranges[-1]

            # Extiende el penúltimo chunk hasta cubrir el último
            ranges[-2] = (prev_start, last_end)
            ranges.pop()

        chunks: List[Chunk] = []

        def format_chunk_content(content: str) -> str:
            if normalize_spaces:
                content = re.sub(r"\s+", " ", content)
            if strip_whitespace:
                content = content.strip()
            return content

        def create_chunk(content: str, ordinal: int) -> Chunk:
            return Chunk(
                chunk_id=str(uuid.uuid4()),
                content=content,
                metadata=ChunkMetadata(
                    source_doc_id=ir.doc_id,
                    chunk_ordinal=ordinal,
                    strategy="fixed_word",
                    original_metadata=ir.metadata,
                ),
            )

        for ordinal, (start_idx, end_idx) in enumerate(ranges):
            start_pos = words_with_positions[start_idx]["start"]
            end_pos = words_with_positions[end_idx - 1]["end"]

            chunk_text = text[start_pos:end_pos]
            chunk_text = format_chunk_content(chunk_text)

            if chunk_text:
                chunks.append(create_chunk(chunk_text, ordinal))

        return chunks
