# Databricks notebook source
# MAGIC %pip install pypdf>=5.0.0 azure-search-documents>=11.6.0 azure-cosmos>=4.6.0
# MAGIC %restart_python

# COMMAND ----------

from config.settings import settings
from src.core.models import AnalyzedDocument
from src.infrastructure.connections import DocumentIntelligenceConnection, DBFSMountPoint
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from src.utils.app_logger import get_logger, configure_logging
from src.utils.async_helpers import run_coro, upload_report
from dataclasses import asdict
from typing import Dict, List, Optional
import json, re, os

# COMMAND ----------

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s04 - Lectura de imágenes."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
DOC_ID    = "meta-id-2026-04-13"

di_client = DocumentIntelligenceConnection(
    endpoint=settings.azure_document_intelligence_endpoint,
    key=settings.azure_document_intelligence_key,
)
st_account = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)
# ======================================================

# COMMAND ----------

# ============ LOGICA PRINCIPAL ====================
async def opening_metadata(storage_account: str, medallion: str, doc_id: str) -> Dict[str, List[str]]:
    """Abre el archivo de metadatos a traves de CosmosDB."""

    cos_client = get_cosmos_client()
    database = get_cosmos_database(cos_client)
    cosmos = CosmosDB(
        db = database,
        container_name = settings.azure_cosmos_container,
        doc_id = doc_id
    )
    structured_paths = await cosmos.get_paths_by_group(storage_account, medallion, "images", doc_id)
    data = {
        "images" : [path for path in structured_paths if path.endswith(".png") or path.endswith(".jpg") or path.endswith(".jpeg") or path.endswith(".tiff")]
    }
    return data

# COMMAND ----------

def load_image_files(storage_account: DBFSMountPoint, data: Dict[str, List[str]] = None) -> Dict[str, bytes]:
    """Descarga imágenes desde el Storage Account.

    Args:
        container: Nombre del contenedor en Data Lake.
        timeout: Tiempo máximo de descarga por archivo (segundos).
        sorted_by_size: Si es True, ordena las imágenes por tamaño ascendente.
        limit: Cantidad máxima de imágenes a procesar.

    Returns:
        Diccionario ``{ruta_archivo: bytes_contenido}``.
    """

    if len(data.values()) == 0:
        logger.warning("⚠️ No se encontraron imágenes en el repositorio.")
        return {}

    image_collection: Dict[str, bytes] = {}
    for file_path in data["images"]:
        logger.info(f"👁️ Descargando imagen: {file_path}")
        content = storage_account.read_file(directory=file_path)
        if content:
            image_collection[file_path] = content
            logger.info(
                f"✅ Descarga de {file_path} completada ({len(content)} bytes)"
                )
        else:
            logger.warning(f"⚠️ Archivo vacío o error al leer: {file_path}")

    logger.info(f"📦 Total imágenes descargadas: {len(image_collection)}")
    return image_collection

# COMMAND ----------

def analyze_image(file_path: str, image_bytes: bytes, model: str = "prebuilt-read") -> Optional[AnalyzedDocument]:
    """Analiza una imagen con Azure Document Intelligence.

    Extrae texto, párrafos, tablas y metadata de la imagen
    (screenshot de documento, JSON, etc.).

    Args:
        file_path: Ruta original del archivo (para logging y metadata).
        image_bytes: Contenido de la imagen en bytes.
        model: Modelo de DI a utilizar. Defaults to ``"prebuilt-read"``.

    Returns:
        AnalyzedDocument con los resultados, o None si falla el análisis.
    """
    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else "unknown"
    logger.info(f"🔍 Analizando imagen: {file_path} (ext={ext}, model={model})")

    try:
        result = di_client.analyze_document_from_stream(
            document=image_bytes,
            file_type=ext,
            model=model,
        )
        logger.info(
            f"✅ Análisis completado: {file_path} "
            f"| páginas={result.pages} "
            f"| párrafos={len(result.paragraphs)} "
            f"| tablas={len(result.tables)}"
        )
        return result
    except Exception as e:
        logger.error(f"❌ Error al analizar {file_path}: {e}")
        return None

# COMMAND ----------

def analyze_all_images(image_collection: Dict[str, bytes], model: str = "prebuilt-read") -> Dict[str, AnalyzedDocument]:
    """Analiza todas las imágenes descargadas.

    Args:
        image_collection: Diccionario ``{ruta: bytes}`` de ``load_image_files``.
        model: Modelo de DI a utilizar.

    Returns:
        Diccionario ``{ruta: AnalyzedDocument}`` con resultados exitosos.
    """
    results: Dict[str, AnalyzedDocument] = {}
    total = len(image_collection)

    for idx, (file_path, image_bytes) in enumerate(image_collection.items(), start=1):
        logger.info(f"📄 [{idx}/{total}] Procesando: {file_path}")
        analyzed_document = analyze_image(file_path, image_bytes, model=model)
        if analyzed_document:
            results[file_path] = analyzed_document

    logger.info(
        f"🏁 Análisis finalizado: {len(results)}/{total} imágenes procesadas correctamente"
    )
    return results

# COMMAND ----------

if __name__ == "__main__":

    # Logs.
    configure_logging()
    logger = get_logger(STEP_NAME)
    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")

    # Inicializacion de Cosmos.
    cos_client = get_cosmos_client()
    cos_db = get_cosmos_database(cos_client)
    cosmos = CosmosDB(
        db = cos_db,
        container_name = settings.azure_cosmos_container,
        doc_id = DOC_ID
    )

# COMMAND ----------

    # Carga de metadata.
    helper_data = run_coro(
        opening_metadata(
            storage_account=settings.azure_storage_account_name,
            medallion=MEDALLION, 
            doc_id=DOC_ID
        )
    )

# COMMAND ----------

    # Carga de imágenes.
    image_collection = load_image_files(storage_account=st_account, data=helper_data)
    if image_collection != {}:
        results = analyze_all_images(image_collection)
        image_results = {}
        # Para debugging.
        for path, doc in results.items():
            logger.info(
                f"\n--- Resultado: {path} ---\n"
                f"  Contenido (primeros 200 chars): {doc.content[:200]}...\n"
                f"  Tablas encontradas: {len(doc.tables)}\n"
                f"  Metadata: {doc.metadata}"
            ) 
            
            try:
                # Guardado en ADLS.
                file_name = path.rsplit('/', 1)[-1].rsplit('.', 1)[0]
                output_path = f"silver/images/{file_name}_analyzed.json"
                payload_bytes = json.dumps(asdict(doc), ensure_ascii=False).encode('utf-8')
                st_account.write_file(directory=output_path, content=payload_bytes)
                logger.info(f"💾 Imagen extraída guardada en ADLS: {output_path}")
                
                image_results[path] = {
                    "status": "completed",
                    "silver_path": output_path,
                    "pages": doc.pages,
                    "content_length": len(doc.content),
                    "file_type": doc.file_type
                }
            except Exception as e:
                logger.error(f"❌ Error al exportar data imagen {path}: {e}")
                image_results[path] = {"status": "error", "error": str(e)}

        # Metadata de analisis.
        report = {
            "id": cosmos._doc_id,
            "azstapropdev": {
                "bronze": {
                    "image_results": image_results,
                    "summary": {
                        "total_processed": len(results)
                    }
                }
            }
        }
    
        # Subida de reporte a Cosmos
        run_coro(upload_report(
            container="azstapropdev",
            medallion="bronze",
            document=report,
            doc_id=cosmos._doc_id
        ))
        logger.info("✅ Reporte final de imágenes subido a Cosmos DB")
    else:
        logger.warning("⚠️ No hay imágenes para analizar.")
