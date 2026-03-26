from config.settings import settings
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
import src.steps.s00_read_files as s00
import src.steps.s01_extract_ocr as s01

def main():
    """Flujo principal de configuración y lectura."""
    logger = get_logger(__name__)
    logger.info(f"--- Iniciando paso: {s00.STEP_NAME} ---")
    
    # Configuración del contenedor y directorio a leer
    CONTAINER = "bronze"
    DIRECTORY = "/" 
    
    # 1. Definir conexiones
    storage = StorageAccount(
        account_name=settings.azure_storage_account_name,
        account_key=settings.azure_storage_account_key
    )

    # 2. Listar archivos
    try:
        paths = storage.list_files(container=CONTAINER, directory=DIRECTORY)
        logger.info(f"Encontrados {len(paths)} archivos en {CONTAINER}/{DIRECTORY}")
    except Exception as e:
        logger.error(f"Error accediendo a Storage Account: {e}")
        return

    # 3. Procesar iterativamente cada archivo
    for file_path in paths:
        group, ext = s00.classify_file_by_extension(file_path)
        logger.info(f"\n--- Procesando: {file_path} [Grupo: {group}, Extensión: {ext}] ---")
    
    grouped_paths = s00.group_files_by_extension(paths)
    logger.info(f"Archivos agrupados por extensión: {grouped_paths}")

    # ================================
    # PASO 2: Extracción OCR Nativa
    # ================================
    logger.info(f"--- Iniciando despliegue de OCR: {s01.STEP_NAME} ---")
    
    ocr_results = s01.process_ocr_for_paths(paths)
    logger.info(f"Resumen de OCR completado: {ocr_results}")
    
    return ocr_results

if __name__ == "__main__":
    configure_logging()
    main()