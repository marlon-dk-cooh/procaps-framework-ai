# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from config.settings import settings
import pandas as pd
import chardet, json, re, io

# ========== CARGA DE SETTINGS ==========
STEP_NAME = "s03_tabular_processing"

csv = r"\.csv$"
xlsx = r"\.xlsx$"
xls = r"\.xls$"

st_account = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key
)

with open("./azpocdk/grouped_paths.json", "r") as f:
    data = json.load(f)

# ========== LOGICA PRINCIPAL ==========

def load_csv_files(container="bronce", timeout=300, **kwargs):
    """Carga archivos csv desde el Storage Account."""

    if kwargs["sorted_by_size"] and kwargs['limit']:
        logger.info(f"⌚ Cargando los primeros {kwargs['limit']} archivos csv ordenados por tamaño.")
        data["tabular"] = sorted(data["tabular"][:kwargs["limit"]], key=lambda x: st_account.get_file_size(container=container, file_path=x))
        logger.info("✅ Archivos ordenados por tamaño")

    csv_collection = {}
    for file in data["tabular"]:
        if re.search(csv, file, re.I):
            logger.info(f"👁️ Leyendo archivo: {file}")
            from_asdl = st_account.read_file(container=container, file_path=file, timeout=timeout)
            read_file = io.BytesIO(from_asdl)
            csv_collection[file] = read_file
            logger.info(f"✅ Carga de {file} completada")

    return csv_collection

def read_csv_files(file_name: str, file_buffer: io.BytesIO) -> pd.DataFrame:
    """Reads a CSV file from a BytesIO buffer, auto-detecting encoding and delimiter.
    
    Args:
        file_name: Original file path (used only for logging).
        file_buffer: The in-memory bytes buffer of the file.
        
    Returns:
        A pandas DataFrame, or None if the file could not be read.
    """
    # 1. Detect encoding from a sample of the raw bytes
    raw = file_buffer.getvalue()
    detected_encoding = chardet.detect(raw[:50000]).get("encoding") or "utf-8"
    logger.info(f"🔍 Encoding detectado para '{file_name}': {detected_encoding}")
    
    # 2. Detect delimiter by sniffing the first decoded line
    try:
        first_line = raw.split(b"\n")[0].decode(detected_encoding, errors="replace")
        sniffer = __import__("csv").Sniffer()
        detected_delimiter = sniffer.sniff(first_line, delimiters=",;\t|").delimiter
    except Exception:
        # Fallback to comma if sniffing fails
        detected_delimiter = ","
    logger.info(f"🔍 Delimiter detectado para '{file_name}': {repr(detected_delimiter)}")
    
    # 3. Read with detected params
    try:
        file_buffer.seek(0)
        df = pd.read_csv(
            file_buffer,
            encoding=detected_encoding,
            sep=detected_delimiter,
            on_bad_lines="warn"  # Skip malformed rows instead of crashing
        )
        logger.info(f"✅ '{file_name}' leído correctamente. Shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"❌ No se pudo leer '{file_name}': {e}")
        return None
    

if __name__ == "__main__":
    configure_logging()
    logger = get_logger(STEP_NAME)
    csv_collection = load_csv_files(sorted_by_size=True, limit=9)
    
    for file_buffer in list(csv_collection.values())[:1]:
        logger.info(f"Pandas: {pd.read_csv(file_buffer, encoding='latin-1')}")