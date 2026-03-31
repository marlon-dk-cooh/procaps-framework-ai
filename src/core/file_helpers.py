"""Utilidades para clasificación y agrupación de archivos por extensión."""
from typing import Tuple
from collections import defaultdict
import re

FILE_GROUPS = {
    "textual" : ["pdf", "docx", "txt", "pptx"],
    "tabular" : ["csv", "xlsx", "xls"],
    "images" : ["png", "jpg", "jpeg", "tiff"],
    "structured": ["json", "parquet", "delta"]
}

def classify_file_by_extension(filename: str) -> Tuple[str, str]:
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

def proportion_by_file_group(paths: list[str], return_pct: bool = False) -> dict[str, float]:
    """
    Calcula el porcentaje de archivos por grupo de extensión.
    
    Args:
        paths = lista de rutas de archivos
            Ej: ['bronze/raw/document.pdf', 'bronze/raw/image.png']

    Returns:
        dict[str, float] = diccionario con porcentajes por grupo de extensión
            Ej: {'textual': 50.0, 'images': 50.0}
    """
    grouped_paths = group_files_by_extension(paths)
    len_group = {x:len(y) for x,y in grouped_paths.items()}
    if return_pct:
        total_files = len(paths)
        return {x: (y/total_files)*100 for x,y in len_group.items()}
    return len_group

def ext_in_structured(
    grouped_path: dict[str, list[str]], 
    export_json: bool = False, 
    export_path: str = None
) -> dict[str, list[int]]:
    """
    Realiza un resumen del tipo de extensiones clasificadas como 'structured'.
    
    Args:
        grouped_path = Grupo de archivos clasificados por extensión que viene de `group_files_by_extension`
            Ej: {'textual': ['bronze/raw/document.pdf'], 'images': ['bronze/raw/image.png']}
    Returns:
        dict[str, int] = diccionario con extensiones y cantidad de ellas.
            Ej: {'.delta' : 45, '.dll' : 21, ...}
    """
    # Expresión regular para buscar una extensión alfabética/numérica al final de la ruta
    # p. ej. .json, .parquet, .delta.
    json = r"\.json$"
    parquet = r"\.parquet$"
    delta = r"\.delta$"

    # Dict
    ext_details = defaultdict(int)
    if "structured" in grouped_path:
        for file_path in grouped_path["structured"]:
            if re.search(json, file_path):
                ext_details["json"] += 1
            elif re.search(parquet, file_path):
                ext_details["parquet"] += 1
            elif re.search(delta, file_path):
                ext_details["delta"] += 1
            else:
                ext_details["other_structured"] +=1

    if export_json and export_path:
        with open(export_path, 'w') as f:
            json.dump(dict(ext_details), f, indent=4)
                
    return dict(ext_details)

def ext_in_others(
    grouped_path: dict[str, list[str]], 
    export_json: bool = False, 
    export_path: str = None
) -> dict[str, list[int]]:
    """
    Realiza un resumen del tipo de extensiones clasificadas como 'otras'.

    Args:
        grouped_path = Grupo de archivos clasificados por extensión que viene de `group_files_by_extension`
            Ej: {'textual': ['bronze/raw/document.pdf'], 'images': ['bronze/raw/image.png']}
    Returns:
        dict[str, list[str]] = diccionario con extensiones y cantidad de ellas.
            Ej: {'.delta' : 45, '.dll' : 21, ...}
    """
    # Expresión regular para buscar una extensión alfabética/numérica al final de la ruta
    # p. ej. .AAZZDF99Z, .pdf, .txt, .123, etc.
    long_ext = r"\.([a-zA-Z0-9]+)$"

    # Dict
    ext_details = defaultdict(int)
    if "other" in grouped_path:
        for file_path in grouped_path["other"]:
            match = re.search(long_ext, file_path)
            if match:
                ext_details["sap_metadata"] += 1
            else:
                ext_details["no_extension"] +=1

    if export_json and export_path:
        with open(export_path, 'w') as f:
            json.dump(dict(ext_details), f, indent=4)
                
    return dict(ext_details)