# Databricks notebook source

import io
import json
from typing import List, Dict
from dataclasses import asdict
from collections import defaultdict
from pypdf import PdfReader, PdfWriter
from config.settings import settings
from src.core.models import AnalyzedDocument
from src.infrastructure.connections import DBFSMountPoint, DocumentIntelligenceConnection
from src.utils.app_logger import get_logger, configure_logging
from src.core.file_helpers import classify_file_by_extension

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s01 - Extracción de datos mediante OCR."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
logger = get_logger(STEP_NAME)

storage = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)
doc_intel = DocumentIntelligenceConnection(
    endpoint=settings.azure_document_intelligence_endpoint,
    key=settings.azure_document_intelligence_key
)

# ================ LOGICA PRINCIPAL ==================
def opening_metadata(helper_file: str = "grouped_ext.json") -> Dict[str, List[str]]:
    """Abre el archivo de metadatos."""
    helpers_path = "/Workspace/Users/marlon.marin@dataknow.co/bundles/procaps-framework-ai/src/steps/helpers"
    helper_file = helpers_path + "/" + helper_file
    with open(helper_file, "r") as f:
        data = json.load(f)
    return data

def requires_document_intelligence(helper_data: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Define si es necesario realizar lectura por OCR a archivos
    
    Args:
        helper_data: Diccionario con las rutas a verificar.
        `ej: {"raw/invoices/invoice_001.pdf": "raw/invoices/invoice_002.pdf"}`
        
    Returns:
        Dict mapeando la ruta original de archivo evaluado al archivo _analyzed.json guardado.
        `ej: {"raw/invoices/invoice_001.pdf": "raw/invoices/invoice_001_analyzed.json"}`
    """
    results_summary = defaultdict(list)
    try:
        paths_for_ocr = {group:path for group, path in helper_data.items() if group in ("textual", "images")}
        logger.info("Usando metadata grouped_ext.json para filtrar archivos OCR.")
    except Exception as e:
        logger.error(f"Metadata no disponible. {e}")

    for paths in paths_for_ocr.values():
        for path in paths:
            group, ext = classify_file_by_extension(path)
            if ext not in "txt":       
                logger.info(f"Ruta '{path}' del grupo: {group}, soportado para OCR.")
                # Si pasó el filtro de arriba, significa que SÍ es válido para OCR
                results_summary[ext].append(path)

    with open("results_summary.json", "w") as f:
        f.write(json.dumps(dict(results_summary), indent=2))

    return dict(results_summary)

def process_with_ocr(
    ocr_paths: Dict[str, List[str]], 
    st_account: DBFSMountPoint = storage,
    doc_intel: DocumentIntelligenceConnection = doc_intel,
    max_pages_per_request: int = 2000
) -> Dict[str, any]:

    """Lee cada archivo desde ADLS y extrae todo el contenido del texto.

    Args:
        ocr_paths: Diccionario con las rutas de los archivos a procesar.
            e.g. {"pdf": ["path/to/doc1.pdf", ...], "png": ["path/to/img.png"]}
        st_account: Instancia de DBFSMountPoint.
        doc_intel: Instancia de DocumentIntelligenceConnection.
        max_pages_per_request: Número máximo de páginas por solicitud.
        
    Returns:
        Diccionario con los resultados del procesamiento.
    """

    results = {}

    for ext, file_paths in ocr_paths.items():
        for file_path in file_paths:
            try:
                logger.info(f"📄 Leyendo bytes desde ADLS: {file_path}")
                file_bytes = st_account.read_file(directory=file_path)

                if not file_bytes:
                    logger.warning(f"⚠️ Archivo vacio, saltando: {file_path}")
                    continue

                # Confirmar conteo de paginas.
                page_count = doc_intel.count_pages(file_bytes)
                logger.info(f"📊 {file_path} tiene {page_count} pagina(s).")

                if page_count > max_pages_per_request:
                    logger.warning(
                        f"⚠️ {file_path} tiene {page_count} paginas (excede {max_pages_per_request}). "
                        f"Iniciando procesamiento por batches."
                    )
                    analyzed = batch_processing(
                        file_path=file_path,
                        file_bytes=file_bytes,
                        doc_intel=doc_intel,
                        max_pages_per_request=max_pages_per_request,
                    )
                else:
                    logger.info(f"✅ Procesando {file_path}...")
                    analyzed = doc_intel.analyze_document_from_stream(
                        document=file_bytes,
                        file_type=ext,
                    )

                # Fallback a batch si el resultado vino vacío (None o 0 páginas).
                if analyzed is None or analyzed.pages == 0:
                    logger.warning(
                        f"⚠️ Resultado vacío para {file_path}. Reintentando via batch_processing."
                    )
                    analyzed = batch_processing(
                        file_path=file_path,
                        file_bytes=file_bytes,
                        doc_intel=doc_intel,
                        max_pages_per_request=max_pages_per_request,
                    )

                if analyzed is not None:
                    results[file_path] = {
                        "path": file_path,
                        "analyzed_document": analyzed,
                        "status": "completed",
                        "pages": analyzed.pages,
                        "content_length": len(analyzed.content),
                    }
                else:
                    results[file_path] = {
                        "path": file_path,
                        "analyzed_document": None,
                        "status": "error",
                        "error": "batch_processing returned None — todos los batches fallaron.",
                    }

            except Exception as e:
                logger.error(f"❌ Fallo al procesar {file_path}: {e}")
                results[file_path] = {"status": "error", "error": str(e)}

    return results

def batch_processing(
    file_path: str,
    file_bytes: bytes,
    doc_intel: DocumentIntelligenceConnection = doc_intel,
    max_pages_per_request: int = 2000,
) -> AnalyzedDocument | None:
    """Procesa un documento grande dividiéndolo en batches de páginas.

    Divide el PDF en fragmentos de ``max_pages_per_request`` páginas usando
    ``pypdf``, envía cada fragmento a Document Intelligence de forma
    secuencial y fusiona todos los ``AnalyzedDocument`` resultantes en uno
    único. Compatible con el límite de 2.000 páginas / 500 MB de la API.

    Args:
        file_path: Ruta del archivo (usada solo para logging y extensión).
        file_bytes: Contenido raw del PDF en bytes.
        doc_intel: Instancia de DocumentIntelligenceConnection.
        max_pages_per_request: Tamaño máximo de cada batch en páginas.

    Returns:
        ``AnalyzedDocument`` fusionado con todos los batches, o
        ``None`` si todos los batches fallan.
    """
    ext = file_path.rsplit(".", 1)[-1].lower()

    # Lectura con PyPDF
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        total_pages = len(reader.pages)
    except Exception as e:
        logger.error("❌ No se pudo leer el PDF %s con pypdf: %s", file_path, e)
        return None

    # Calculo de rangos de batch
    ranges = [
        (start, min(start + max_pages_per_request, total_pages))
        for start in range(0, total_pages, max_pages_per_request)
    ]
    logger.info(
        "📦 %s → %d páginas divididas en %d batch(es).",
        file_path, total_pages, len(ranges),
    )

    # -- 3. Procesar cada batch ---------------------------------------------
    partial_results: list[AnalyzedDocument] = []

    for batch_num, (start, end) in enumerate(ranges, start=1):
        logger.info(
            "🔄 Batch %d/%d: páginas %d–%d de %s",
            batch_num, len(ranges), start + 1, end, file_path,
        )

        # Serializar el fragmento de páginas a bytes en memoria
        writer = PdfWriter()
        for page_idx in range(start, end):
            writer.add_page(reader.pages[page_idx])

        buffer = io.BytesIO()
        writer.write(buffer)
        chunk_bytes = buffer.getvalue()

        try:
            partial = doc_intel.analyze_document_from_stream(
                document=chunk_bytes,
                file_type=ext,
            )
            partial_results.append(partial)
            logger.info(
                "✅ Batch %d completado: %d páginas, %d chars.",
                batch_num, partial.pages, len(partial.content),
            )
        except Exception as e:
            logger.error(
                "❌ Batch %d falló para %s: %s", batch_num, file_path, e
            )
            # Continúa con el siguiente batch en lugar de abortar.

    if not partial_results:
        logger.error("❌ Todos los batches fallaron para %s.", file_path)
        return None

    # Fusión de chunks.
    merged = AnalyzedDocument(
        content="\n".join(r.content for r in partial_results),
        pages=sum(r.pages for r in partial_results),
        file_type=ext,
        paragraphs=[p for r in partial_results for p in r.paragraphs],
        tables=[t for r in partial_results for t in r.tables],
        metadata=partial_results[0].metadata,  # metadatos del primer batch
    )
    logger.info(
        "🏁 Fusión completada para %s: %d páginas totales, %d chars.",
        file_path, merged.pages, len(merged.content),
    )
    return merged

def generate_analyzed_document(analyzed: AnalyzedDocument) -> Dict[str, any]:
    """Genera un diccionario con los resultados del procesamiento."""
    

def save_results(results: Dict[str, any]):
    """Guarda los resultados del procesamiento en un archivo JSON."""
    with open("results.json", "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    configure_logging()
    result_summary = requires_document_intelligence(helper_data=opening_metadata())
    results = process_ocr_files(ocr_paths=result_summary)
    logger.info(results)
