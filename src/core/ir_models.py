"""
Intermediate Representation (IR) Models
---------------------------------------
Defines the shared data models used across the ingestion pipeline,
including normalized documents, structured elements, chunk metadata,
and final chunks ready for embedding or indexing. These models provide
the common contract between extraction, normalization, chunking, and
search-related stages of the workflow.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class Element:
    """
    Represents a structured part of a document (e.g. heading, paragraph, table row).
    Used for structural and hierarchical chunking.
    """

    type: str  # "heading", "paragraph", "table", "code_block", "list_item"
    text: str
    level: Optional[int] = None  # For headings (1-6) or list depth
    page_number: Optional[int] = None  # For PDF/DOCX
    section_path: Optional[str] = None  # Breadcrumb: "Introduction > Overview"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentIR:
    """
    Canonical Intermediate Representation of a document.
    """

    doc_id: str
    source_path: str

    # Content representations (at least one should be present)
    plain_text: Optional[str] = None
    markdown_text: Optional[str] = None
    structured_elements: Optional[List[Element]] = None

    # Document-level metadata (author, created_at, etc.)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChunkMetadata:
    """
    Standardized metadata for a generated chunk.
    """

    source_doc_id: str
    chunk_ordinal: int
    strategy: str

    # Hierarchical context
    parent_chunk_id: Optional[str] = None
    section_path: Optional[str] = None
    level: Optional[int] = None

    # Source pointers
    page_number: Optional[int] = None
    char_start: Optional[int] = None
    char_end: Optional[int] = None

    # Original metadata carried over
    original_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Chunk:
    """
    Final chunk output ready for embedding/indexing.
    """

    chunk_id: str
    content: str
    metadata: ChunkMetadata
