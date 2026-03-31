# Databricks notebook source
from src.core.file_helpers import (
    classify_file_by_extension, 
    group_files_by_extension, 
    proportion_by_file_group,
    ext_in_structured, 
    ext_in_others
)
from src.utils.app_logger import get_logger, configure_logging
from src.infrastructure.connections import StorageAccount
from config.settings import settings
import os
import json
# ======== CARGA DE SETTINGS =============
STEP_NAME = "s00_read_files"

# ======== LOGICA PRINCIPAL ==============
def main(container: str, directory: str, **kwargs):
    # Logs
    logger = get_logger(STEP_NAME)
    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")
    
    # 1. Definir conexiones
    storage = StorageAccount(
        account_name=settings.azure_storage_account_name,
        account_key=settings.azure_storage_account_key
    )

    # 2. Listar archivos
    try:
        paths = storage.list_files(container=container, directory=directory)
        logger.info(f"Encontrados {len(paths)} archivos en {container}/{directory}")
    except Exception as e:
        logger.error(f"Error accediendo a Storage Account: {e}")
        return

    # 3. Procesar cada archivo con classify_file_by_extension
    for file_path in paths:
        group, ext = classify_file_by_extension(file_path)
        logger.info(f"\n--- Procesando: {file_path} [Grupo: {group}, Extensión: {ext}] ---")
    
    # Archivos clasificados por extensión
    grouped_paths = group_files_by_extension(paths) 
    
    # Porcentaje de archivos por grupo
    percent_group = proportion_by_file_group(paths)
    
    # Extensiones en la categoria de "structured", segun la clasificacion en FILE_GROUPS.
    ext_struc_paths = ext_in_structured(grouped_paths)

    # Extensiones en la categoria de "others", segun la clasificacion en FILE_GROUPS.
    ext_other_paths = ext_in_others(grouped_paths)

    # Escribir archivos con los resultados (por ahora en local).
    os.makedirs(kwargs["output_path"], exist_ok=True)

    if os.path.exists(kwargs["output_path"]):
        with open(os.path.join(kwargs["output_path"], kwargs["extension_in_others_class"]), "w") as f:
            f.write(json.dumps(ext_other_paths, indent=2))
        with open(os.path.join(kwargs["output_path"], kwargs["len_per_group_extension"]), "w") as f:
            f.write(json.dumps(percent_group, indent=2))
        with open(os.path.join(kwargs["output_path"], kwargs["grouped_extension"]), "w") as f:
            f.write(json.dumps(grouped_paths, indent=4))
        with open(os.path.join(kwargs["output_path"], kwargs["extension_in_structured_class"]), "w") as f:
            f.write(json.dumps(ext_struc_paths, indent=2))

if __name__ == "__main__":
    # Logs
    configure_logging()
    # Datalake Procaps
    main(
            container="bronce", 
            directory="/",
            output_path="./azpocdk", 
            extension_in_others_class="output.json", 
            len_per_group_extension="len_per_group.json", 
            grouped_extension="grouped_paths.json",
            extension_in_structured_class="structured_ext.json" # New structured resume output.
    )