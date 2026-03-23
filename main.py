from config.settings import settings
from src.infrastructure.connections import StorageAccount
from src.utils.app_logger import get_logger, configure_logging
import src.steps.s00_read_files as s00

def main():
    """Flujo principal de configuración y lectura."""
    logger = get_logger(__name__)
    logger.info(f"Iniciando paso: {s00.STEP_NAME}")
    
    # Configuración del contenedor y directorio a leer
    CONTAINER = "bronze"
    DIRECTORY = "raw" 
    
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

if __name__ == "__main__":
    configure_logging()
    main()