# Databricks notebook source
# MAGIC %pip install pypdf>=5.0.0 azure-search-documents>=11.6.0 azure-cosmos>=4.6.0
# MAGIC %restart_python

# COMMAND ----------

from src.infrastructure.connections import DBFSMountPoint
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from src.utils.app_logger import get_logger, configure_logging
from src.utils.async_helpers import run_coro, upload_report
from src.core.file_helpers import filter_by_size
from typing import Dict, Any, List
from config.settings import settings
import pandas as pd
import concurrent.futures, json, re, io, sys

# COMMAND ----------

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s02 - Procesamiento de datos estructurados."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
DOC_ID    = "meta-id-2026-04-13"

storage_account = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)
# ======================================================

# COMMAND ----------

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
        structured_paths = await cosmos.get_paths_by_group(storage_account, medallion, "structured", doc_id)
        return {
            "parquet" : [path for path in structured_paths if path.endswith(".parquet")],
            "json" : [path for path in structured_paths if path.endswith(".json")]
        }
    finally:
        await cos_client.close()

# COMMAND ----------

def load_parquet_files(storage_account: DBFSMountPoint, data: dict, size_limit: int = None):
    """
    Carga archivos parquet desde el punto de montura.
        Args:
            storage_account: Punto de montura del Storage Account.
            data: Diccionario con las rutas de los archivos.
            size_limit: Tamaño máximo en MB para filtrar los archivos.
        Returns:
            Colección de archivos parquet cargados.
    """
    logger = get_logger("⌚ Cargando archivos parquet.")
    parquet_collection = {}

    parquet_files = data["parquet"]
    if size_limit:
        filtered_loading_files = filter_by_size(
                storage_account=storage_account, 
                paths=parquet_files, 
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
            parquet_collection[file] = read_file
            logger.info(f"✅ Carga de {file} completada")

    return parquet_collection

# COMMAND ----------

def load_json_files(storage_account: DBFSMountPoint, data: dict, size_limit: int = None):
    """
    Carga archivos json desde el Storage Account.
        Args:
            data: Diccionario con las rutas de los archivos.
            size_limit: Tamaño máximo en MB para filtrar los archivos.
            **kwargs: Argumentos adicionales.
        Returns:
            Colección de archivos json cargados.
    """
    logger = get_logger("⌚ Cargando archivos json.")
    json_collection = {}

    json_files = data["json"]
    if size_limit:
        filtered_loading_files = filter_by_size(
                storage_account=storage_account, 
                paths=json_files, 
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
            json_collection[file] = read_file
            logger.info(f"✅ Carga de {file} completada")

    return json_collection

# COMMAND ----------

def generate_metadata(cosmos: CosmosDB, df: pd.DataFrame, output_path: str) -> Dict[str, Any]:
    """
    Genera metadatos para CosmosDB.
        Args:
            cosmos: Objeto CosmosDB.
            df: DataFrame con los datos procesados.
            output_path: Ruta de salida para el archivo de metadatos.
        Returns:
            Diccionario con los metadatos generados.
    """
    logger = get_logger("⌚ Generando metadatos.")
    logger.info("⏳ Generando metadatos para CosmosDB.")
    output_path = output_path.replace("dbfs:/",)

    report = {
        "id" : cosmos._doc_id,
        CONTAINER: {
            MEDALLION: {
                "structured_results": {
                    "status": "completed",
                    "silver_path": output_path,
                    "rows_processed": len(df),
                    "columns": list(df.columns)
                }
            }
        }
    }

    return report

# COMMAND ----------

if __name__ == "__main__": 
    # logs
    configure_logging()

    # Inicializacion de Cosmos
    cos_client = get_cosmos_client()
    cos_db = get_cosmos_database(cos_client)
    cosmos = CosmosDB(
        db = cos_db,
        container_name = settings.azure_cosmos_container,
        doc_id = None
    )


# COMMAND ----------

    # Corutinas
    helper_data = run_coro(
        opening_metadata(
            storage_account=settings.azure_storage_account_name,
            medallion=MEDALLION,
            doc_id=DOC_ID
        )
    )

# COMMAND ----------

    # Limite de 0.5 MB (para pruebas)
    size_limit = 500
    parquet_collection = load_parquet_files(
            storage_account=storage_account, 
            data=helper_data, 
            size_limit=size_limit
        )

# COMMAND ----------

    json_collection = load_json_files(
            storage_account=storage_account, 
            data=helper_data, 
            size_limit=size_limit
        )

# COMMAND ----------

    # # Leer todos los archivos parquet de la colección y concatenarlos en un solo DataFrame.
    # for buf in parquet_collection.values():
    #     buf.seek(0)

    # # TODO: Buscar otra opcion que no sea concatenar todos los .parquet.
    # df = pd.concat([pd.read_parquet(buf) for buf in parquet_collection.values()], ignore_index=True)

# COMMAND ----------

    # Guardar los archivos parquet condensados en un solo parquet.
    output_path = "silver/structured/merged_structured_data.parquet"
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    st_account.write_file(directory=output_path, content=parquet_buffer.getvalue())
    logger.info(f"💾 DataFrame consolidado guardado en ADLS: {output_path}")

# COMMAND ----------

    report = generated_metadata(cosmos=cosmos, df=df, output_path=output_path)
    # Subir reporte a Cosmos.
    run_coro(upload_report(
        doc_id=cosmos._doc_id,
        storage_account=CONTAINER,
        medallion=MEDALLION,
        document=report
    ))
    logger.info("✅ Reporte subido a Cosmos")
    logger.info(f"Columnas del DataFrame: {df.columns}")
