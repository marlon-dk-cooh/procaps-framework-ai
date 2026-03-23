# Databricks notebook source
from typing import Tuple
from collections import defaultdict

# ======== CARGA DE SETTINGS =============
STEP_NAME = "s00_read_files"

# ============== HELPERS =================
FILE_GROUPS = {
    "textual" : ["pdf", "docx", "txt", "pptx"],
    "tabular" : ["csv", "xlsx", "xls"],
    "images" : ["png", "jpg", "jpeg", "tiff"],
    "structured": ["json", "parquet", "delta"]
 }

def classify_file_by_extension(filename:str) -> Tuple[str, str]:
    """
    Clasifica un archivo según su extensión.
    
    Args:
        filename = nombre de archivo 
            Ej: 'document.pdf'

    Returns:
        Tuple[str, str] = (group, extension)
            Ej: ('textual', 'pdf')
            Ej: ('images', 'png')
    """
    ext = filename.lower().split(".")[-1] if '.' in filename else ''

    for group, extensions in FILE_GROUPS.items():
        if ext in extensions:
            return group, ext

    # Si no se encuentra en archivos conocidos, se clasifica como "other"
    return "other", ext

def group_files_by_extension(paths: list[str]) -> dict[str, list[str]]:
    """
    Agrupa una lista de rutas de archivos por su extensión.
    
    Args:
        paths = lista de rutas de archivos
            Ej: ['bronze/raw/document.pdf', 'bronze/raw/image.png']

    Returns:
        dict[str, list[str]] = diccionario con rutas agrupadas por extensión
            Ej: {'textual': ['bronze/raw/document.pdf'], 'images': ['bronze/raw/image.png']}
    """
    grouped_paths = defaultdict(list)
    for path in paths:
        group, _ = classify_file_by_extension(path)
        grouped_paths[group].append(path)
    return dict(grouped_paths)

