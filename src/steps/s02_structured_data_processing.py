# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from config.settings import settings
import pandas as pd
import concurrent.futures, json, re, io

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

def load_parquet_files(timeout: int, **kwargs):

    logger = get_logger(STEP_NAME)
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")
    parquet_collection = []

    if kwargs["sorted_by_size"] and kwargs['limit']:
        logger.info(f"⌚ Cargando los primeros {kwargs['limit']} archivos parquet ordenados por tamaño.")
        data["structured"] = sorted(data["structured"][:kwargs["limit"]], key=lambda x: st_account.get_file_size(container="bronce", file_path=x))
        logger.info("✅ Archivos ordenados por tamaño.")

    for file in data["structured"]:
        if re.search(parquet, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(
                container="bronce", file_path=file, timeout=timeout
            )
            if not from_asdl:
                logger.warning(f"⏳ Archivo vacío o timeout al leer '{file}'. Saltando...")
                continue
                
            read_file = io.BytesIO(from_asdl)
            parquet_collection.append(read_file)
            logger.info(f"✅ Carga de {file} completada")

    return parquet_collection

def load_json_files(timeout: int, **kwargs):
    logger = get_logger(STEP_NAME)
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")
    json_collection = []

    for file in data["structured"]:
        if re.search(json_re, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(
                container="bronce", file_path=file, timeout=timeout
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
    parquet_collection = load_parquet_files(timeout=60, limit=50, sorted_by_size=True)

    # Leer todos los archivos parquet de la colección y concatenarlos en un solo DataFrame.
    df = pd.concat([pd.read_parquet(f) for f in parquet_collection], ignore_index=True)
    df.to_csv("./azpocdk/all_parquet_files.csv")
    logger.info(f"Columnas del DataFrame: {df.columns}")
