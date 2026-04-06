# Databricks notebook source
import json
from typing import List, Dict
from dataclasses import asdict
from collections import defaultdict

from config.settings import settings
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

def process_ocr_files(
    ocr_paths: Dict[str, List[str]], 
    st_account: DBFSMountPoint = storage,
    doc_intel: DocumentIntelligenceConnection = doc_intel,
    max_pages_per_request: int = 2000
) -> Dict[str, any]:

    """Lee cada archivo desde ADLS, verifica el número de páginas y procesa a través de Document Intelligence.
    Si un documento excede las 2000 páginas, se marca para procesamiento por lotes.

    Args:
        ocr_paths: Diccionario con las rutas de los archivos a procesar.
            e.g. {"pdf": ["path/to/doc1.pdf", ...], "png": ["path/to/img.png"]}
        container: Nombre del contenedor de ADLS.
    
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
                        f"Procesamiento por batches requerido!."
                    )
                    # Archivos que deben ser procesados por lotes.
                    results[file_path] = {
                        "status": "pending_batch",
                        "pages": page_count,
                        "batches_needed": (page_count // max_pages_per_request) + 1,
                    }
                else:
                    logger.info(f"✅ Procesando {file_path}...")
                    analyzed = doc_intel.analyze_document_from_stream(
                        document=file_bytes,
                        file_type=ext,
                        model="prebuilt-read"
                    )
                    results[file_path] = {
                        "status": "completed",
                        "pages": page_count,
                        "content_length": len(analyzed.content),
                    }

            except Exception as e:
                logger.error(f"❌ Fallo al procesar {file_path}: {e}")
                results[file_path] = {"status": "error", "error": str(e)}

    return results

def batch_processing(
    file_path: str, 
    st_account: DBFSMountPoint = storage, 
    doc_intel: DocumentIntelligenceConnection = doc_intel,
    max_pages_per_request: int = 2000
):
    # #TODO: Implementar logica de procesamiento por lotes.
    # Dividir el documento en trozos de MAX_PAGES_PER_REQUEST paginas,
    # enviar cada trozo a doc_intel.analyze_document_from_stream(),
    # y fusionar los resultados de AnalyzedDocument.
    pass

# #TODO: How is the file being read, is the model correct?
def model_selection():
    pass


if __name__ == "__main__":
    configure_logging()
    result_summary = requires_document_intelligence(helper_data=opening_metadata())
    results = process_ocr_files(ocr_paths=result_summary)
    logger.info(results)
