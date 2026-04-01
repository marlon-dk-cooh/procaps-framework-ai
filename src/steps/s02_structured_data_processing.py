# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from src.core.file_helpers import filter_by_size
from config.settings import settings
import pandas as pd
import concurrent.futures, json, re, io, sys

# ========== CARGA DE SETTINGS ===========
STEP_NAME = "s02_structured_data_processing"
parquet = r"\.parquet$"
json_re = r"\.json$"
st_account = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key
)

with open("./azpocdk/grouped_paths.json", "r") as f:
    data = json.load(f)

# ========== LOGICA PRINCIPAL ==============

def load_parquet_files(timeout: int, container: str = "bronze", size_limit: int = None, **kwargs):
    """Carga archivos parquet desde el Storage Account."""
    func_name = sys._getframe().f_code.co_name
    logger = get_logger(f"{STEP_NAME}.{func_name}")
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")
    parquet_collection = []

    if size_limit:
        data["structured"] = filter_by_size(data["structured"], size_limit)
        logger.info(f"✅ Se seleccionaron {len(data['structured'])} archivos para procesar.")

    for file in data["structured"]:
        if re.search(parquet, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(
                container=container, file_path=file, timeout=timeout
            )
            if not from_asdl:
                logger.warning(f"⏳ Archivo vacío o timeout al leer '{file}'. Saltando...")
                continue
                
            read_file = io.BytesIO(from_asdl)
            parquet_collection.append(read_file)
            logger.info(f"✅ Carga de {file} completada")

    return parquet_collection

def load_json_files(timeout: int, container: str = "bronze", size_limit: int = None, **kwargs):
    """Carga archivos json desde el Storage Account."""
    func_name = sys._getframe().f_code.co_name
    logger = get_logger(f"{STEP_NAME}.{func_name}")
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")
    json_collection = []

    if size_limit:
        data["structured"] = filter_by_size(data["structured"], size_limit)
        logger.info(f"✅ Se seleccionaron {len(data['structured'])} archivos para procesar.")

    for file in data["structured"]:
        if re.search(json_re, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(
                container=container, file_path=file, timeout=timeout
            )
            if not from_asdl:
                logger.warning(f"⏳ Archivo vacío o timeout al leer '{file}'. Saltando...")
                continue
                
            read_file = io.BytesIO(from_asdl)
            json_collection.append(read_file)
            logger.info(f"✅ Carga de {file} completada")

    return json_collection

if __name__ == "__main__":
    configure_logging()
    # Limite de 5 MB
    size_limit = 5000
    parquet_collection = load_parquet_files(timeout=60, size_limit=size_limit)

    # Leer todos los archivos parquet de la colección y concatenarlos en un solo DataFrame.
    df = pd.concat([pd.read_parquet(f) for f in parquet_collection], ignore_index=True)
    df.to_csv("./azpocdk/all_parquet_files.csv")
    logger.info(f"Columnas del DataFrame: {df.columns}")
