# Databricks notebook source
import json
from typing import List, Dict
from dataclasses import asdict

from config.settings import settings
from src.steps.s00_read_files import classify_file_by_extension
from src.infrastructure.connections import StorageAccount, DocumentIntelligenceConnection
from src.utils.app_logger import get_logger

# ======== CARGA DE SETTINGS =============
STEP_NAME = "s01_extract_ocr"
logger = get_logger(STEP_NAME)


def process_ocr_for_paths(paths: List[str]) -> Dict[str, str]:
    """Recorre los archivos y directorios, donde analiza por OCR los archivos soportados
    y guarda los resultados en archivos JSON al lado de los originales.
    
    Args:
        paths: Lista con las rutas a verificar.
        `ej: ["raw/invoices/invoice_001.pdf", "raw/invoices/invoice_002.pdf"]`
        
    Returns:
        Dict mapeando la ruta original de archivo evaluado al archivo _analyzed.json guardado.
        `ej: {"raw/invoices/invoice_001.pdf": "raw/invoices/invoice_001_analyzed.json"}`
    """
    logger.info(f"Iniciando extracción OCR para {len(paths)} rutas.")
    
    CONTAINER = "bronze"
    results_summary = {}

    # Define resource handles
    storage = StorageAccount(
        account_name=settings.azure_storage_account_name,
        account_key=settings.azure_storage_account_key
    )
    
    doc_intel = DocumentIntelligenceConnection(
        endpoint=settings.azure_document_intelligence_endpoint,
        key=settings.azure_document_intelligence_key
    )

    for file_path in paths:
        if file_path.endswith("_analyzed.json"):
            continue
            
        group, ext = classify_file_by_extension(file_path)
        
        # Filtrar estrictamente solo imágenes, textuales o tabulares
        if group not in ("textual", "images", "tabular"):
            logger.debug(f"Ignorando '{file_path}' (Grupo: {group}). No soportado nativamente para OCR.")
            continue
            
        logger.info(f"Extrayendo OCR: {file_path} [Grupo: {group}, Modelo Ideal: {'prebuilt-layout' if group == 'tabular' else 'prebuilt-read'}]")
        model_name = "prebuilt-layout" if group == "tabular" else "prebuilt-read"
        
        try:
            logger.debug("Descargando bytes de ADLS...")
            file_bytes = storage.read_file(container=CONTAINER, file_path=file_path)
            
            logger.debug("Enviando bytes a Azure Document Intelligence...")
            result = doc_intel.analyze_document_from_stream(
                document=file_bytes, 
                file_type=ext, 
                model=model_name
            )
            
            # Volcar a JSON (Utilizando Dataclasses AsDict)
            result_dict = asdict(result)
            json_str = json.dumps(result_dict, indent=2, ensure_ascii=False)
            logger.info(f"\n{json_str}\n")
            
            # # Exportar archivo procesado al lado del original
            out_filename = f"{file_path.rsplit('.', 1)[0]}_analyzed.json"
            return {
                "file_path": out_filename,
                "json_content": json_str,
            }
            # storage.write_file(
            #     container=CONTAINER, 
            #     file_path=out_filename, 
            #     data=json_str.encode("utf-8"), 
            #     overwrite=True
            # )
            # logger.info(f"✅ Extracción correcta. Output estructurado guardado en Storage: {out_filename}")
            # results_summary[file_path] = out_filename
            
        except Exception as e:
            logger.error(f"❌ Fallo crítico de OCR para el archivo {file_path}: {e}")
            
    return results_summary



