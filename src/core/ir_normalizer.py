"""
IR Normalizer
-------------
Converts raw file content into the workflow's canonical `DocumentIR`
representation. It detects supported file types, extracts their
content, and normalizes plain text, structure, and metadata into a
consistent intermediate format for downstream processing.
"""

import os
import mimetypes
from typing import Optional
import uuid

from .ir_models import DocumentIR, Element
from .extraction import FileExtractor
from .logging_config import get_logger

logger = get_logger(__name__)


class IRNormalizer:
    """
    Normalizes input files to DocumentIR.
    """

    def __init__(self, file_extractor: Optional[FileExtractor] = None):
        self.extractor = (
            file_extractor or FileExtractor()
        )  # Defaults if not provided, but ideally injected

    def _normalize_md(self, file_path: str, mime_type: str, doc_id: str) -> DocumentIR:
        """
        Parses Markdown to extract plain text AND structured elements (headers, code blocks).
        """
        content = self.extractor.extract(file_path, mime_type)

        elements = []
        import re

        lines = content.split("\n")

        # Simple parser for headers and code blocks
        in_code_block = False

        for line in lines:
            stripped = line.strip()

            # Code blocks
            if stripped.startswith("```"):
                in_code_block = not in_code_block
                elements.append(Element(type="code_block_delimiter", text=line))
                continue

            if in_code_block:
                elements.append(Element(type="code_line", text=line))
                continue

            # Headers
            match = re.match(r"^(#+)\s+(.*)", line)
            if match:
                level = len(match.group(1))
                text = match.group(2)
                elements.append(Element(type="heading", text=text, level=level))
            elif stripped:
                elements.append(Element(type="paragraph", text=stripped))

        return DocumentIR(
            doc_id=doc_id,
            source_path=file_path,
            plain_text=content,
            markdown_text=content,
            structured_elements=elements,
            metadata={
                "mime_type": mime_type,
                "normalization_method": "markdown_parsing",
            },
        )

    def normalize(self, file_path: str, mime_type: Optional[str] = None) -> DocumentIR:
        """
        Main entry point. Detects type and directs to specific normalizer.
        """
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                # Fallback by extension
                ext = os.path.splitext(file_path)[1].lower()
                if ext == ".txt":
                    mime_type = "text/plain"
                elif ext == ".md":
                    mime_type = "text/markdown"
                elif ext == ".json":
                    mime_type = "application/json"

        doc_id = str(uuid.uuid4())

        try:
            if mime_type == "text/plain":
                return self._normalize_txt(file_path, mime_type, doc_id)
            elif mime_type == "text/markdown":
                return self._normalize_md(file_path, mime_type, doc_id)
            elif mime_type == "application/json":
                return self._normalize_json(file_path, mime_type, doc_id)
            else:
                return self._normalize_generic_text(file_path, mime_type, doc_id)

        except Exception as e:
            logger.error(f"Normalization failed for {file_path}: {e}")
            raise e

    def _normalize_txt(self, file_path: str, mime_type: str, doc_id: str) -> DocumentIR:
        content = self.extractor.extract(file_path, mime_type)
        return DocumentIR(
            doc_id=doc_id,
            source_path=file_path,
            plain_text=content,
            metadata={
                "mime_type": mime_type,
                "normalization_method": "txt_pass_through",
            },
        )

    def _normalize_generic_text(
        self, file_path: str, mime_type: str, doc_id: str
    ) -> DocumentIR:
        # Uses existing extraction logic which handles PDF/DOCX/OCR
        content = self.extractor.extract(file_path, mime_type)
        return DocumentIR(
            doc_id=doc_id,
            source_path=file_path,
            plain_text=content,
            metadata={
                "mime_type": mime_type,
                "normalization_method": "generic_extraction",
            },
        )

    def _normalize_json(
        self, file_path: str, mime_type: str, doc_id: str
    ) -> DocumentIR:
        """
        Parses JSON array into DocumentIR with records in metadata.
        Typical use case: list of logs/records.
        """
        import json

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Expecting a list of dicts. If dict, wrap in list?
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            records = data
        else:
            # Maybe primitive?
            records = [{"value": data}]

        return DocumentIR(
            doc_id=doc_id,
            source_path=file_path,
            plain_text=json.dumps(data, indent=2),  # Fallback text
            metadata={
                "mime_type": mime_type,
                "normalization_method": "json_records",
                "records": records,  # Store records for RecordChunker
            },
        )
