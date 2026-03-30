from config.settings import settings
from src.infrastructure.connections import StorageAccount, DocumentIntelligenceConnection
from src.utils.app_logger import get_logger, configure_logging
from src.core.file_helpers import (
    classify_file_by_extension, group_files_by_extension, proportion_by_file_group, ext_in_others
)
import json

def main():
    """Flujo principal de configuración y lectura."""
    logger = get_logger(__name__)
    logger.info("--- Iniciando paso: s00_read_files ---")
    
    # Configuración del contenedor y directorio a leer
    CONTAINER = "bronce"
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
        group, _ = classify_file_by_extension(file_path)
        # logger.info(f"\n--- Procesando: {file_path} [Grupo: {group}, Extensión: {ext}] ---")
    
    grouped_paths = group_files_by_extension(paths)
    percent_group = proportion_by_file_group(paths)
    ext_paths = ext_in_others(grouped_paths)
        
    # logger.info(f"Archivos agrupados por extensión: {grouped_paths}")
    with open("output.json", "w") as f:
        f.write(json.dumps(ext_paths, indent=2))
    with open("len_per_group.json", "w") as f:
        f.write(json.dumps(percent_group, indent=2))
    with open("grouped_paths.json", "w") as f:
        f.write(json.dumps(grouped_paths, indent=4))

    # ================================
    # PASO 2: Extracción OCR Nativa (Refactored)
    # ================================
    logger.info("--- Iniciando despliegue de OCR: s01_extract_ocr ---")

    # doc_intel = DocumentIntelligenceConnection(
    #     endpoint=settings.azure_document_intelligence_endpoint,
    #     key=settings.azure_document_intelligence_key
    # )

    # # Initialize collectors outside the loop
    # num_pages_per_file = {}
    # all_results = {}
    # failed_files = []

    for file_path in paths:
        group, ext = classify_file_by_extension(file_path)
        
        # #TODO: Improve this decision route!!!
        if group not in ("textual", "images"): # OCR solo lee texto e imagenes.
            logger.info(f"Ignorando '{file_path}'. No soportado.")
            
        # model_name = "prebuilt-layout" if group == "tabular" else "prebuilt-read"
    
    #     try:    
    #         file_bytes = storage.read_file(container=CONTAINER, file_path=file_path)
            
    #         result = doc_intel.analyze_document_from_stream(
    #             document=file_bytes, 
    #             file_type=ext, 
    #             model=model_name
    #         )

    #         # Actualizar diccionarios.
    #         num_pages_per_file[file_path] = doc_intel.count_pages(file_bytes, model=model_name)
    #         all_results[file_path] = asdict(result)
        
    #     except Exception as e:
    #         logger.error(f"❌ Fallo de OCR para {file_path}: {e}")
    #         failed_files.append({"path": file_path, "error": str(e)})
    #         continue

    # # ================================
    # # PASO 3: Persistencia de Resultados
    # # ================================
    # # Write only once after the loop finishes
    # try:
    #     with open("num_pages.json", "w") as f:
    #         json.dump(num_pages_per_file, f, indent=4)

    #     with open("result_dict.json", "w") as f:
    #         json.dump(all_results, f, indent=4)

    #     if failed_files:
    #         with open("failed_files.json", "w") as f:
    #             json.dump(failed_files, f, indent=4)
    #         logger.warning(f"Proceso completado con {len(failed_files)} errores. Ver failed_files.json.")
    #     else:
    #         logger.info("✅ OCR completado exitosamente para todos los archivos.")

    # except Exception as e:
    #     logger.error(f"Error guardando los manifiestos finales: {e}") 

if __name__ == "__main__":
    configure_logging()
    main()