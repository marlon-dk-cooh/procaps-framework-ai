# Databricks notebook source
# MAGIC %pip install pypdf>=5.0.0 azure-search-documents>=11.6.0 azure-cosmos>=4.6.0
# MAGIC %restart_python

# COMMAND ----------

from src.core.file_helpers import classify_file_by_extension
from src.infrastructure.connections import DBFSMountPoint
from src.infrastructure.cosmos import (
    CosmosDB, 
    build_metadata_document, 
    get_cosmos_client, 
    get_cosmos_database
)
from src.utils.app_logger import get_logger, configure_logging
from src.utils.async_helpers import run_coro, upload_report
from config.settings import settings
import os, re, json, asyncio

# ================ CARGA DE SETTINGS ==================

# COMMAND ----------

STEP_NAME = "s00 - Carga de datos."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"

cos_client = get_cosmos_client()
database = get_cosmos_database(cos_client)

cosmos = CosmosDB(
    db = database,
    container_name = settings.azure_cosmos_container,
    doc_id = None
)
# =====================================================

# ================ LOGICA PRINCIPAL ===================

# COMMAND ----------

def create_metadata(container: str, medallion: str, directory: str):
    
    # Definicion de conexiones a traves de punto de montura.
    storage = DBFSMountPoint(container=container, medallion=medallion)

    # Listar archivos
    try:
        paths = storage.list_files(directory=directory)
        logger.info(f"Encontrados {len(paths)} archivos en {directory}:")
    except Exception as e:
        logger.error(f"Error accediendo a Storage Account: {e}")
        return

    # Procesamiento de cada archivo con classify_file_by_extension y get_file_size para clasificar y obtener el tamaño de cada archivo.
    file_sizes = {}
    for file_path in paths:
        group, ext = classify_file_by_extension(file_path)
        size_file = storage.get_file_size(directory=file_path)
        file_sizes[file_path] = size_file
        logger.info(f"\n--- Procesando: {file_path} [Grupo: {group}, Extensión: {ext}, Tamaño: {size_file} kB] ---")

    # Creacion de documento para subir a instancia de Cosmos.
    new_document = build_metadata_document(
        doc_id=cosmos._doc_id,
        container=container,
        medallion=medallion,
        paths=paths,
        file_sizes=file_sizes
    )

    logger.info(f"Documento creado: {new_document[container]}")

    return new_document

# COMMAND ----------

if __name__ == "__main__":
    # Logs
    configure_logging()
    logger = get_logger(STEP_NAME)
    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")

# COMMAND ----------

    metadata = create_metadata(
        container=CONTAINER,
        medallion=MEDALLION,
        directory=""
    )

# COMMAND ----------

    # Subiendo metadatos a CosmosDB.
    run_coro(
        upload_report(
            container=settings.azure_storage_account_name,
            medallion=MEDALLION,
            document=metadata,
            doc_id=cosmos._doc_id,
            splittable_key="grouped_path" 
        )
    )
    logger.info(f"Documento creado: {metadata[CONTAINER]}")
