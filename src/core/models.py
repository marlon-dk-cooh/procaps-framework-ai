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
