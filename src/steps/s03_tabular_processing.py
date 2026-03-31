# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from config.settings import settings
import pandas as pd
import json
import re
import io

# Logging
logger = get_logger("s03_tabular_processing")

# Basic configs.
csv = r"\.csv$"
xlsx = r"\.xlsx$"
xls = r"\.xls$"

st_account = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key
)

with open("./azpocdk/grouped_paths.json", "r") as f:
    data = json.load(f)


def main():
    # Read all csv files from the collection and concatenate them into a single DataFrame.
    csv_collection = []
    for file in data["tabular"]:
        if re.search(csv, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(container="bronce", file_path=file, timeout=20)
            read_file = io.BytesIO(from_asdl)
            csv_collection.append(read_file)
            logger.info(f"✅ Carga de {file} completada")
    
    columns = {}
    for file in csv_collection:
        try:
            columns[file] = pd.read_csv(file).columns.tolist()
            logger.info("✅ Columnas cargadas.")
        except Exception as e:
            logger.error(f"Error al cargar las columnas del archivo {file}: {e}")
            continue

    return columns

if __name__ == "__main__":
    configure_logging()
    columns = main()

    with open("./azpocdk/columns.json", "w") as f:
        json.dump(columns, f, indent=4)
    logger.info("✅ Columnas guardadas.")