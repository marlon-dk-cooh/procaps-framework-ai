# Databricks notebook source
# MAGIC %pip install pypdf>=5.0.0 azure-search-documents>=11.6.0 azure-cosmos>=4.6.0
# MAGIC %restart_python

# COMMAND ----------

import io, asyncio
import json
from typing import List, Dict
from dataclasses import asdict
from collections import defaultdict
from pypdf import PdfReader, PdfWriter
from config.settings import settings
from src.core.models import AnalyzedDocument
from src.infrastructure.connections import DBFSMountPoint, DocumentIntelligenceConnection
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from src.utils.app_logger import get_logger, configure_logging
from src.utils.async_helpers import run_coro, upload_report
from src.core.file_helpers import classify_file_by_extension

# COMMAND ----------

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s01 - Extracción de datos mediante OCR."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
DOC_ID    = "meta-id-2026-04-13"
logger = get_logger(STEP_NAME)

storage = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)
doc_intel = DocumentIntelligenceConnection(
    endpoint=settings.azure_document_intelligence_endpoint,
    key=settings.azure_document_intelligence_key
)

# COMMAND ----------

# ================ LOGICA PRINCIPAL ==================
async def opening_metadata(storage_account: str, medallion: str, doc_id: str) -> Dict[str, List[str]]:
    """Abre el archivo de metadatos desde CosmosDB."""
    try:
        cos_client = get_cosmos_client()
        database = get_cosmos_database(cos_client)
        cosmos = CosmosDB(
            db = database,
            container_name = settings.azure_cosmos_container,
            doc_id = doc_id
        )
        textual_paths = await cosmos.get_paths_by_group(storage_account, medallion, "textual", doc_id)
        image_paths = await cosmos.get_paths_by_group(storage_account, medallion, "images", doc_id)
        return {"textual": textual_paths, "images": image_paths}
    finally:
        await cos_client.close()

# COMMAND ----------

def requires_document_intelligence(helper_data: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Define si es necesario realizar lectura por OCR a archivos
    
    Args:
        helper_data: Diccionario con las rutas a verificar.
        `ej: {"textual" : ["raw/invoices/invoice_001.pdf", "raw/invoices/invoice_002.pdf"],
            "images" : ["raw/images/image_001.png", "raw/images/image_002.png"]}`
        
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

    return dict(results_summary)

# COMMAND ----------

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
                    try:
                        # Extract basic filename to use as the JSON target
                        file_name = file_path.rsplit('/', 1)[-1].rsplit('.', 1)[0]
                        output_path = f"silver/ocr/{file_name}_analyzed.json"
                        
                        # Serialize AnalyzedDocument to JSON bytes
                        payload_bytes = json.dumps(asdict(analyzed), ensure_ascii=False).encode('utf-8')
                        st_account.write_file(directory=output_path, content=payload_bytes)
                        logger.info(f"💾 Extracción guardada en ADLS: {output_path}")
                    except Exception as e:
                        logger.error(f"❌ Error guardando extracción en DBFS/ADLS para {file_path}: {e}")

                    results[file_path] = {
                        "path": file_path,
                        "silver_path": output_path,
                        "analyzed_document": asdict(analyzed),
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

# COMMAND ----------

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

# COMMAND ----------

def generate_ocr_report(
    results: Dict[str, any],
    container: str = CONTAINER,
    medallion: str = MEDALLION,
    doc_id: str | None = None,
) -> Dict[str, any]:
    """Genera un documento de reporte para Cosmos a partir de la salida de ``process_with_ocr``.

    Estructura del documento generado::

        {
            "id": "<doc_id>",
            "azstapropdev": {
                "bronze": {
                    "ocr_results": { <per-file summaries> },
                    "summary": { totals }
                }
            },
            "updated_at": "<ISO-8601>"
        }

    Args:
        results: Diccionario retornado por ``process_with_ocr``.
        container: Nombre del contenedor de storage.
        medallion: Capa del datalake.
        doc_id: ID del documento Cosmos. Si es None se genera automáticamente.

    Returns:
        Documento listo para ``cosmos.upsert_metadata``.
    """

                    #     results[file_path] = {
                    #     "path": file_path,
                    #     "analyzed_document": analyzed,
                    #     "status": "completed",
                    #     "pages": analyzed.pages,
                    #     "content_length": len(analyzed.content),
                    # }
    from datetime import datetime, timezone

    if doc_id is None:
        doc_id = f"ocr-report-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

    # Resumenes por archivo.
    ocr_results: Dict[str, any] = {}
    total_pages = 0
    total_chars = 0
    succeeded = 0
    failed = 0

    for file_path, entry in results.items():
        status = entry.get("status", "unknown")

        if status == "completed":
            succeeded += 1
            pages = entry.get("pages", 0)
            content_length = entry.get("content_length", 0)
            analyzed: Dict[str, Any] | None = entry.get("analyzed_document")

            total_pages += pages
            total_chars += content_length

            ocr_results[file_path] = {
                "status": status,
                "silver_path": entry.get("silver_path"),
                "pages": pages,
                "content_length": content_length,
                "paragraphs_count": len(analyzed.get("paragraphs", [])),
                "tables_count": len(analyzed.get("tables", [])),
                "file_type": analyzed.get("file_type", None),
                "metadata": analyzed.get("metadata", {}),
            }
        else:
            failed += 1
            ocr_results[file_path] = {
                "status": status,
                "error": entry.get("error", "unknown"),
            }

    report = {
        "id": doc_id,
        container: {
            medallion: {
                "ocr_results": ocr_results,
                "summary": {
                    "total_files": len(results),
                    "succeeded": succeeded,
                    "failed": failed,
                    "total_pages": total_pages,
                    "total_chars": total_chars,
                },
            }
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "📋 Reporte OCR generado — archivos: %d, exitosos: %d, fallidos: %d, páginas: %d",
        len(results), succeeded, failed, total_pages,
    )
    return report

# COMMAND ----------

if __name__ == "__main__":

    # Logs
    configure_logging()

    # Corutinas
    helper_data = run_coro(opening_metadata(
        storage_account=settings.azure_storage_account_name,
        medallion=MEDALLION,
        doc_id=DOC_ID
    ))

# COMMAND ----------

    result_summary = requires_document_intelligence(helper_data=helper_data)

# COMMAND ----------

    # Salida de resultados por procesamiento de OCR.
    results = process_with_ocr(ocr_paths=result_summary)
    # Escritura en contenedor "silver" de ADLS.
    write_operation_instance = DBFSMountPoint(container=CONTAINER, medallion="silver")
# COMMAND ----------

    write_operation_instance.write_file(directory="ocr", content=json.dumps(results, ensure_ascii=False).encode('utf-8'))

# COMMAND ----------

    # Generar reporte
    report = generate_ocr_report(results)
    logger.info(f"✅ Reporte generado {report}")

# COMMAND ----------

    # Subir reporte de metadata a Cosmos.
    report_to_cosmos = upload_report(
        doc_id="ocr-report-2026-04-12",
        container=CONTAINER,
        medallion=MEDALLION,
        document=report
    )
    run_coro(report_to_cosmos)
    logger.info("✅ Reporte subido a Cosmos")
