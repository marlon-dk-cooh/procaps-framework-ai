# Databricks notebook source
from src.core.file_helpers import (
    classify_file_by_extension, 
    group_files_by_extension, 
    proportion_by_file_group,
    ext_in_structured, 
    ext_in_others
)
from src.utils.app_logger import get_logger, configure_logging
from src.infrastructure.connections import DBFSMountPoint
from config.settings import settings
import os, re
import json

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s00 - Carga de datos."
CONTAINER = "azstapropdev"
MEDALLION = "bronze"
# =====================================================

# ================ LOGICA PRINCIPAL ===================
def main(container: str, medallion: str, directory: str, **kwargs):
    # Logs
    logger = get_logger(STEP_NAME)
    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")
    
    # 1. Definir conexiones
    storage = DBFSMountPoint(container=container, medallion=medallion)

    # 2. Listar archivos
    try:
        paths = [storage.get_file_size(directory=path) for path in storage.list_files(directory=directory)]
        logger.info(f"Encontrados {len(paths)} archivos en {directory}:")
    except Exception as e:
        logger.error(f"Error accediendo a Storage Account: {e}")
        return

    return paths

    # # 3. Procesar cada archivo con classify_file_by_extension y obtener el tamaño.
    # file_sizes = {}
    # for file_path in paths:
    #     group, ext = classify_file_by_extension(file_path)
    #     size_file = storage.get_file_size(directory=file_path)
    #     file_sizes[file_path] = f"{size_file} kB"
    #     logger.info(f"\n--- Procesando: {file_path} [Grupo: {group}, Extensión: {ext}, Tamaño: {size_file} kB] ---")
    
    # # Archivos clasificados por extensión
    # grouped_paths = group_files_by_extension(paths) 
    
    # # Porcentaje de archivos por grupo
    # percent_group = proportion_by_file_group(paths)

    # # Extensiones en la categoria de "structured", segun la clasificacion en FILE_GROUPS.
    # ext_struc_paths = ext_in_structured(grouped_paths)

    # # Extensiones en la categoria de "others", segun la clasificacion en FILE_GROUPS.
    # ext_other_paths = ext_in_others(grouped_paths)

    # # Escribir archivos con los resultados (por ahora en local).
    # os.makedirs(kwargs["output_path"], exist_ok=True)

    # if os.path.exists(kwargs["output_path"]):
    #     with open(os.path.join(kwargs["output_path"], kwargs["extension_in_others_class"]), "w") as f:
    #         f.write(json.dumps(ext_other_paths, indent=2))
    #     with open(os.path.join(kwargs["output_path"], kwargs["len_per_group_extension"]), "w") as f:
    #         f.write(json.dumps(percent_group, indent=2))
    #     with open(os.path.join(kwargs["output_path"], kwargs["grouped_extension"]), "w") as f:
    #         f.write(json.dumps(grouped_paths, indent=4))
    #     with open(os.path.join(kwargs["output_path"], kwargs["extension_in_structured_class"]), "w") as f:
    #         f.write(json.dumps(ext_struc_paths, indent=2))
    #     with open(os.path.join(kwargs["output_path"], kwargs["file_sizes"]), "w") as f:
    #         f.write(json.dumps(file_sizes, indent=2))

if __name__ == "__main__":
    # Logs
    configure_logging()
    # Datalake Procaps
    paths = main(
            container=CONTAINER,
            medallion=MEDALLION,
            directory="",
            # output_path="./helpers", 
            # extension_in_others_class="others_ext.json", 
            # len_per_group_extension="len_per_group.json", 
            # grouped_extension="grouped_ext.json",
            # extension_in_structured_class="structured_ext.json",
            # file_sizes="file_sizes.json"
    )

    for path in paths:
        logger.info(f"Ruta: {path}")

