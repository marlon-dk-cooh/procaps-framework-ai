"""
Markdown Chunker
----------------
Splits Markdown files by headers and preserves hierarchy.
If a section is too large, it is further split using recursive-like separators.
"""

from typing import List, Dict, Any
import uuid
import re

from ..ir_models import DocumentIR, Chunk, ChunkMetadata
from .base import ChunkerStrategy, ChunkingConfig


class MarkdownChunker(ChunkerStrategy):
    """
    Splits content based on Markdown structure.

    Behavior:
    - If structured_elements are available, builds sections from headings.
    - Otherwise falls back to regex-based Markdown header parsing.
    - Preserves hierarchy in section_path.
    - If a section exceeds chunk_size, splits it into smaller chunks
      using markdown-friendly separators and overlap.
    """

    HEADER_REGEX = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)

    def chunk(self, ir: DocumentIR, config: ChunkingConfig) -> List[Chunk]:
        chunks: List[Chunk] = []

        if ir.structured_elements:
            sections = self._build_sections_from_structured_elements(ir)
        elif ir.markdown_text:
            sections = self._build_sections_from_markdown(ir.markdown_text)
        else:
            return []

        for section in sections:
            content = section["content"].strip()
            if not content:
                continue

            subchunks = self._split_large_section(
                text=content,
                chunk_size=config.chunk_size,
                chunk_overlap=config.chunk_overlap,
            )

            for subchunk in subchunks:
                if not subchunk.strip():
                    continue

                chunks.append(
                    Chunk(
                        chunk_id=str(uuid.uuid4()),
                        content=subchunk,
                        metadata=ChunkMetadata(
                            source_doc_id=ir.doc_id,
                            chunk_ordinal=len(chunks),
                            strategy="markdown",
                            section_path=section["section_path"],
                            original_metadata=ir.metadata,
                        ),
                    )
                )

        return chunks

    def _build_sections_from_structured_elements(
        self, ir: DocumentIR
    ) -> List[Dict[str, Any]]:
        """
        Build sections using IR structured elements.
        Expects heading elements with:
        - el.type == "heading"
        - el.text
        - optional el.level
        """
        sections: List[Dict[str, Any]] = []
        current_lines: List[str] = []
        header_stack: List[str] = []

        for el in ir.structured_elements:
            el_text = (el.text or "").strip()
            if not el_text:
                continue

            if el.type == "heading":
                # Flush previous section
                if current_lines:
                    sections.append(
                        {
                            "section_path": " > ".join(header_stack)
                            if header_stack
                            else "Intro",
                            "content": "\n".join(current_lines).strip(),
                        }
                    )
                    current_lines = []

                level = getattr(el, "level", 1) or 1
                level = max(1, int(level))

                # Adjust hierarchy stack
                while len(header_stack) >= level:
                    header_stack.pop()

                header_stack.append(el_text)

                # Include the markdown heading in chunk content
                current_lines.append(f"{'#' * level} {el_text}")
            else:
                current_lines.append(el_text)

        # Flush final section
        if current_lines:
            sections.append(
                {
                    "section_path": " > ".join(header_stack)
                    if header_stack
                    else "Intro",
                    "content": "\n".join(current_lines).strip(),
                }
            )

        return sections

    def _build_sections_from_markdown(self, markdown_text: str) -> List[Dict[str, Any]]:
        """
        Fallback for raw markdown text using regex headers.
        Preserves heading hierarchy in section_path.
        """
        text = (markdown_text or "").strip()
        if not text:
            return []

        matches = list(self.HEADER_REGEX.finditer(text))

        # No headings found: whole document as one section
        if not matches:
            return [
                {
                    "section_path": "Full Doc",
                    "content": text,
                }
            ]

        sections: List[Dict[str, Any]] = []
        header_stack: List[str] = []

        for i, match in enumerate(matches):
            level = len(match.group(1))
            title = match.group(2).strip()
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)

            section_block = text[start:end].strip()

            while len(header_stack) >= level:
                header_stack.pop()

            header_stack.append(title)

            sections.append(
                {
                    "section_path": " > ".join(header_stack),
                    "content": section_block,
                }
            )

        return sections

    def _split_large_section(
        self, text: str, chunk_size: int, chunk_overlap: int
    ) -> List[str]:
        """
        Split a large section using recursive-like markdown-friendly separators.
        """
        if len(text) <= chunk_size:
            return [text]

        separators = ["\n\n", "\n", ". ", " ", ""]
        return self._recursive_split(
            text=text,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=separators,
            current_separator_index=0,
        )

    def _recursive_split(
        self,
        text: str,
        chunk_size: int,
        chunk_overlap: int,
        separators: List[str],
        current_separator_index: int,
    ) -> List[str]:
        text = text.strip()
        if not text:
            return []

        if len(text) <= chunk_size:
            return [text]

        if current_separator_index >= len(separators):
            return self._hard_split(text, chunk_size, chunk_overlap)

        separator = separators[current_separator_index]

        if separator == "":
            return self._hard_split(text, chunk_size, chunk_overlap)

        parts = text.split(separator)

        # If separator does not split, try next separator
        if len(parts) == 1:
            return self._recursive_split(
                text=text,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separators=separators,
                current_separator_index=current_separator_index + 1,
            )

        chunks: List[str] = []
        current_chunk = ""

        for part in parts:
            part = part.strip()
            if not part:
                continue

            candidate = part if not current_chunk else current_chunk + separator + part

            if len(candidate) <= chunk_size:
                current_chunk = candidate
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())

                if len(part) > chunk_size:
                    nested_chunks = self._recursive_split(
                        text=part,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        separators=separators,
                        current_separator_index=current_separator_index + 1,
                    )
                    chunks.extend(nested_chunks)
                    current_chunk = ""
                else:
                    current_chunk = part

        if current_chunk:
            chunks.append(current_chunk.strip())

        return self._add_overlap(chunks, chunk_overlap)

    def _hard_split(self, text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
        """
        Hard split by characters when no separator works.
        """
        if chunk_size <= 0:
            return [text]

        step = max(1, chunk_size - chunk_overlap)
        chunks: List[str] = []

        for i in range(0, len(text), step):
            chunk = text[i : i + chunk_size].strip()
            if chunk:
                chunks.append(chunk)

            if i + chunk_size >= len(text):
                break

        return chunks

    def _add_overlap(self, chunks: List[str], chunk_overlap: int) -> List[str]:
        """
        Add character overlap between adjacent chunks.
        """
        if not chunks or chunk_overlap <= 0:
            return chunks

        overlapped: List[str] = [chunks[0]]

        for i in range(1, len(chunks)):
            previous = overlapped[-1]
            current = chunks[i]

            overlap_text = (
                previous[-chunk_overlap:] if len(previous) > chunk_overlap else previous
            )
            overlapped.append((overlap_text + current).strip())

        return overlapped
