# Databricks notebook source
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from src.core.file_helpers import filter_by_size
from config.settings import settings
import pandas as pd
import chardet, json, re, io, sys

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

def load_csv_files(container="bronze", timeout=300, size_limit: float = None, **kwargs):
    """Carga archivos csv desde el Storage Account."""
    func_name = sys._getframe().f_code.co_name
    logger = get_logger(f"{STEP_NAME}.{func_name}")
    logger.info(f" --- Iniciando paso: {STEP_NAME} ---")

    if size_limit:
        data["tabular"] = filter_by_size(data["tabular"], size_limit)
        logger.info(f"✅ Se seleccionaron {len(data['tabular'])} archivos para procesar.")

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
        return df
    except Exception as e:
        logger.error(f"❌ No se pudo leer '{file_name}': {e}")
        return None
    

if __name__ == "__main__":
    configure_logging()
    logger = get_logger(STEP_NAME)
    csv_collection = load_csv_files(size_limit=5000)
    
    for file_buffer in list(csv_collection.values())[:1]:
        logger.info(f"Pandas: {pd.read_csv(file_buffer, encoding='latin-1')}")