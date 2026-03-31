# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from config.settings import settings
import pandas as pd
import json
import re
import io
import timeit

# Logging
logger = get_logger("s02_structured_data_processing")

# Basic configs.
parquet = r"\.parquet$"
st_account = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key
)
with open("./azpocdk/grouped_paths.json", "r") as f:
    data = json.load(f)

import concurrent.futures

def main():
    parquet_collection = []
    TIMEOUT_SECONDS = 5
    
    for file in data["structured"]:
        if re.search(parquet, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            
            from_asdl = st_account.read_file(
                container="bronce", file_path=file, timeout=TIMEOUT_SECONDS
            )
            
            if not from_asdl:
                logger.warning(f"⏳ Archivo vacío o timeout al leer '{file}'. Saltando...")
                continue
                
            read_file = io.BytesIO(from_asdl)
            parquet_collection.append(read_file)
            logger.info(f"✅ Carga de {file} completada")


    # Read all parquet files from the collection and concatenate them into a single DataFrame.
    # The resulting DataFrame is then saved as a parquet file in the 'silver' container.
    df = pd.concat([pd.read_parquet(f) for f in parquet_collection], ignore_index=True)
    df.to_csv("./azpocdk/all_parquet_files.csv")
    logger.info(f"Columnas del DataFrame: {df.columns}")
    # try:
    #     st_account.write_file(
    #         container="silver",
    #         file_path="/all_parquet_files.parquet",
    #         file_content=df.to_parquet()
    #     )
    # except Exception as e:
    #     logger.error(f"Error al guardar el archivo: {e}")


if __name__ == "__main__":
    configure_logging()
    main()
