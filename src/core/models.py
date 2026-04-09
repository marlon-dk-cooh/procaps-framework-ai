from dataclasses import dataclass, field
from typing import Any


@dataclass
class AnalyzedDocument:
    """Resultado estructurado de un análisis de Document Intelligence.

    Attributes:
        content: Texto completo extraído en orden de lectura.
        pages: Número total de páginas en el documento.
        file_type: Extensión original del archivo (ej. ``"pdf"``).
        paragraphs: Lista de textos de los párrafos.
        tables: Tablas representadas como ``list[rows[cells]]``,
            donde cada celda (cell) es un string.
        metadata: Diccionario extensible para atributos adicionales
            (ej. ``handwritten``, ``languages``, ``word_count``).
    """
    content: str
    pages: int
    file_type: str
    paragraphs: list[str] = field(default_factory=list)
    tables: list[list[list[str]]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EmbeddingRecord:
    """Registro normalizado listo para embedding, producido por s05_process_files.

    Cada instancia representa un único documento procesado por el pipeline,
    independientemente de su tipo de origen (OCR, tabular, imagen, etc.).

    Attributes:
        id: UUID-5 determinístico derivado de ``origin`` (idempotente entre reruns).
        origin: Ruta original del archivo en el Data Lake / DBFS.
        content: Texto extraído o datos serializados como string.
        metadata: Blob JSON compacto con metadata ligera de trazabilidad.
        update_at: Timestamp ISO-8601 de cuándo se generó el registro.
    """
    id: str
    origin: str
    content: str
    metadata: str          # JSON string
    updated_at: str         # ISO 8601
