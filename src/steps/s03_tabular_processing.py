# Databricks notebook source
# MAGIC %pip install pypdf>=5.0.0 azure-search-documents>=11.6.0 azure-cosmos>=4.6.0
# MAGIC %restart_python

# COMMAND ----------

from src.core.file_helpers import filter_by_size
from src.infrastructure.connections import DBFSMountPoint
from src.utils.app_logger import get_logger, configure_logging
from src.utils.async_helpers import run_coro, upload_report
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from config.settings import settings
import pandas as pd
import chardet, json, re, io, sys
from typing import Dict, Any, List

# COMMAND ----------

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s03 - Procesamiento de datos tabulares."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
DOC_ID    = "meta-id-2026-04-13"

storage_account = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)
# ======================================================

# COMMAND ----------

# ================== LOGICA PRINCIPAL ==================
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
        structured_paths = await cosmos.get_paths_by_group(storage_account, medallion, "tabular", doc_id)
        return {
            "csv": [path for path in structured_paths if path.endswith(".csv")],
            "xlsx": [path for path in structured_paths if path.endswith(".xlsx")],
            "xls": [path for path in structured_paths if path.endswith(".xls")]
        }
    finally:
        await cos_client.close()

# COMMAND ----------

def transform_to_csv(storage_account: DBFSMountPoint, data: Dict[str, List[str]]) -> Dict[str, io.BytesIO]:
    """Transforma archivos tabulares en formatos de Excel (.xlsx, .xls) a memoria CSV en BytesIO.
    Para posteriormente unificarlos a load_csv_files.
    Args:
        storage_account: Instancia de DBFSMountPoint.
        data: Diccionario con rutas de archivos.
    Returns:
        Diccionario con rutas de archivos.
    """
    logger.info("Iniciando transformación de archivos tabulares a CSV...")
    transformed = {}
    
    for ext, paths in data.items():
        if ext in ["xlsx", "xls"]:
            for file in paths:
                logger.info(f"🔄 Transformando a CSV: {file}")
                try:
                    raw_bytes = storage_account.read_file(directory=file)
                    
                    try:
                        df = pd.read_excel(io.BytesIO(raw_bytes))
                    except Exception as excel_err:
                        logger.warning(f"⚠️ Falló lectura Excel ({file}), intentando fallback HTML. Error: {excel_err}")
                        df = pd.read_html(io.BytesIO(raw_bytes))[0]
                    
                    csv_buffer = io.BytesIO()
                    df.to_csv(csv_buffer, index=False, encoding='utf-8')
                    csv_buffer.seek(0)
                    
                    transformed[file] = csv_buffer
                    logger.info(f"✅ Transformación exitosa para: {file}")
                except Exception as e:
                    logger.error(f"❌ Error definitivo transformando {file}: {e}")
                    continue
                        
    return transformed

# COMMAND ----------

def load_csv_files(storage_account: DBFSMountPoint, data: Dict[str, List[str]], size_limit: float = None) -> Dict[str, io.BytesIO]:
    """Carga archivos csv nativos desde metadata a memoria BytesIO.
    Args:
        storage_account: Instancia de DBFSMountPoint.
        data: Lista con rutas de archivos.
        size_limit: Tamaño máximo en MB para filtrar los archivos.
    Returns:
        Diccionario con rutas de archivos.
    """
    logger = get_logger("⌚ Cargando archivos csv.")
    csv_collection = {}

    csv_files = data["csv"]
    if size_limit:
        filtered_loading_files = filter_by_size(
            storage_account=storage_account, 
            paths=csv_files, 
            size_limit=size_limit
        )
        logger.info(f"✅ Se seleccionaron {len(filtered_loading_files)} archivos para procesar.")

    for file in filtered_loading_files:
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = storage_account.read_file(directory=file)
            if not from_asdl:
                logger.warning(f"⏳ Archivo vacío o timeout al leer '{file}'. Saltando...")
                continue

            read_file = io.BytesIO(from_asdl)
            csv_collection[file] = read_file
            logger.info(f"✅ Carga de {file} completada")

    return csv_collection

# COMMAND ----------

def read_csv_files(file_name: str, file_buffer: io.BytesIO) -> csv:
    """Lee un archivo CSV desde un buffer BytesIO, detectando automáticamente la codificación y el delimitador.
    
    Args:
        file_name: Ruta original del archivo (se usa solo para logging).
        file_buffer: El buffer de bytes en memoria del archivo.
        
    Returns:
        Un DataFrame de pandas, o None si el archivo no se pudo leer.
    """

    raw = file_buffer.getvalue()
    detected_encoding = chardet.detect(raw[:50000]).get("encoding") or "utf-8"
    logger.info(f"🔍 Encoding detectado para '{file_name}': {detected_encoding}")
    
    try:
        first_line = raw.split(b"\n")[0].decode(detected_encoding, errors="replace")
        sniffer = __import__("csv").Sniffer()
        detected_delimiter = sniffer.sniff(first_line, delimiters=",;\t|").delimiter
    except Exception:
        detected_delimiter = ","
    logger.info(f"🔍 Delimiter detectado para '{file_name}': {repr(detected_delimiter)}")
    
    try:
        file_buffer.seek(0)
        df = pd.read_csv(
            file_buffer,
            encoding=detected_encoding,
            sep=detected_delimiter,
            on_bad_lines="warn"
        )
        logger.info(f"✅ '{file_name}' leído correctamente. Shape: {df.shape}")
        return df.to_csv(file_name)
    except Exception as e:
        logger.error(f"❌ No se pudo leer '{file_name}': {e}")
        return None

# COMMAND ----------

if __name__ == "__main__":
    # Logs.
    configure_logging()
    logger = get_logger(STEP_NAME)
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")

# COMMAND ----------

    # Inicializacion de Cosmos.
    cos_client = get_cosmos_client()
    cos_db = get_cosmos_database(cos_client)
    cosmos = CosmosDB(
        doc_id=DOC_ID,
        db = cos_db,
        container_name = settings.azure_cosmos_container
    )

# COMMAND ----------

    helper_data = run_coro(
        opening_metadata(
            storage_account=settings.azure_storage_account_name,
            medallion=MEDALLION,
            doc_id=DOC_ID
        )
    )

# COMMAND ----------

    # Carga de metadatos para tipo tabular.
    csv_collection = load_csv_files(storage_account=storage_account, data=helper_data, size_limit=100000)

# COMMAND ----------

    # Procesamiento de extensiones que no son CSV
    transformed_xls = transform_to_csv(storage_account=storage_account, data=helper_data)
    # #TODO: Incluir openpyxl. (Missing optional dependency 'lxml'.  Use pip or conda to install lxml.)

# COMMAND ----------

    # Unificamos 
    csv_collection.update(transformed_xls)

# COMMAND ----------

    # Uniformizacion de csv.
    csv_collection = {k: read_csv_files(k, v) for k, v in csv_collection.items()}

# COMMAND ----------

# Guardado en ADLS.
tabular_results = {}
for origin_path, buffer in csv_collection.items():
    try:
        # Limpieza del nombre del archivo.
        file_name = origin_path.rsplit('/', 1)[-1].rsplit('.', 1)[0]
        output_path = f"silver/tabular/{file_name}_normalized.csv"
            
        # Escritura del archivo en ADLS.
        storage_account.write_file(directory=output_path, content=buffer.getvalue())
        logger.info(f"💾 Guardado CSV normalizado en ADLS: {output_path}")

        # Opcional: contar filas para agregar metadatos.
        buffer.seek(0)
        df_sample = pd.read_csv(buffer, nrows=0, on_bad_lines="skip")
            
        tabular_results[origin_path] = {
            "status": "completed",
            "silver_path": output_path,
            "columns": list(df_sample.columns)
        }
    except Exception as e:
        logger.error(f"❌ Error al exportar data tabular {origin_path}: {e}")
        tabular_results[origin_path] = {
            "status": "error",
            "error": str(e)
        }

# COMMAND ----------

    # Leer documento existente y hacer merge (preserva grouped_path, etc.)
    existing_doc = run_coro(cosmos.get_metadata_by_id(DOC_ID))
    if existing_doc:
        bronze_data = existing_doc.get("azstapropdev", {}).get("bronze", {})
        bronze_data["tabular_results"] = tabular_results
        bronze_data["summary"] = {"total_processed": len(csv_collection)}
        existing_doc["azstapropdev"]["bronze"] = bronze_data
        report = existing_doc
    else:
        report = {
            "id": cosmos._doc_id,
            "azstapropdev": {
                "bronze": {
                    "tabular_results": tabular_results,
                    "summary": {
                        "total_processed": len(csv_collection)
                    }
                }
            }
        }

    # Upsert a Cosmos
    run_coro(upload_report(
        container="azstapropdev",
        medallion="bronze",
        document=report,
        doc_id=cosmos._doc_id
    ))
    logger.info("✅ Reporte final tabular subido a Cosmos DB")
