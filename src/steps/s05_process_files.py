# Databricks notebook source
"""s05 — Procesamiento y Normalización de archivos para embedding.

Lee los reportes de Cosmos DB (ocr_results, structured_results, etc.),
ubica las cargas útiles físicas (archivos .json, .parquet, .csv) persistidas 
en el Datalake (ADLS/DBFS) por los scripts previos s01, s02, s03, y s04,
y extrae/consolida toda la información cruda en un único DataFrame normalizado.

Este DF es exportado como un archivo Parquet listo para ser indexado
por los motores de Azure AI Search en el script s06.
"""
import json
import uuid
import io
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.core.models import EmbeddingRecord
from src.utils.app_logger import get_logger, configure_logging
from src.infrastructure.connections import DBFSMountPoint
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from config.settings import settings

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s05 - Procesamiento de archivos."
UUID_NAMESPACE = uuid.NAMESPACE_URL
logger = get_logger(STEP_NAME)

CONTAINER = "azstapropdev"
MEDALLION = "bronze"
DOC_ID    = "test-split-2026-04-10"

storage_account = DBFSMountPoint(container=CONTAINER, medallion=MEDALLION)

def _compact_metadata(
    metadata_dict: Dict[str, Any],
    source_step: str,
) -> str:
    """Reduce la metadata a un payload de procedencia pequeño.

    Los embeddings se generan solo a partir de ``content``, por lo que mantenemos la metadata ligera
    para evitar hinchar la carga útil del parquet y los documentos de búsqueda posteriores.
    """
    compacted: Dict[str, Any] = {"source_step": source_step}

    for key, value in metadata_dict.items():
        if value in (None, "", [], {}, ()):
            continue

        if key == "columns" and isinstance(value, list):
            compacted["column_count"] = len(value)
            continue

        if isinstance(value, (str, int, float, bool)):
            compacted[key] = value

    return json.dumps(compacted, ensure_ascii=False, separators=(",", ":"))

def _build_record(
    origin: str,
    content: str,
    metadata_dict: Dict[str, Any],
    source_step: str,
) -> EmbeddingRecord:
    """Aplica el estandar global para el almacenamiento indexable."""
    return EmbeddingRecord(
        id=str(uuid.uuid5(UUID_NAMESPACE, origin)),
        origin=origin,
        content=content,
        metadata=_compact_metadata(metadata_dict, source_step),
        updated_at=datetime.now(timezone.utc).isoformat(),
    )

def process_extracted_json(
    results_metadata: Dict[str, Any], 
    st_account: DBFSMountPoint, 
    source_step: str
) -> List[EmbeddingRecord]:
    """Procesa payloads JSON guardados en ADLS originados desde OCR e Imágenes.
    Analiza el apuntador 'silver_path' que dejaron en Cosmos los pasos anteriores.
    """
    func_logger = get_logger(f"{STEP_NAME}.process_extracted_json")
    records: List[EmbeddingRecord] = []

    for file_path, entry in results_metadata.items():
        if entry.get("status") != "completed" or not entry.get("silver_path"):
            func_logger.warning(f"⏭️ Saltando archivo {file_path} (No procesado correctamente).")
            continue
            
        silver_path = entry["silver_path"]
        try:
            raw_bytes = st_account.read_file(directory=silver_path)
            if not raw_bytes:
                func_logger.error(f"❌ El archivo {silver_path} devuelto desde el Datalake fue vacío.")
                continue

            doc_data = json.loads(raw_bytes.decode('utf-8'))
            
            records.append(_build_record(
                origin=file_path,
                content=doc_data.get("content", ""),
                metadata_dict={
                    "file_type": doc_data.get("file_type"),
                    "pages": doc_data.get("pages"),
                    "silver_path": silver_path
                },
                source_step=source_step,
            ))
            func_logger.info(f"✅ Documento normalizado ({source_step}): origen {file_path}")
        except Exception as e:
            func_logger.error(f"❌ Error decodificando payload {silver_path}: {e}")

    return records

def process_structured_metadata(
    struc_meta: Dict[str, Any], 
    st_account: DBFSMountPoint
) -> List[EmbeddingRecord]:
    """Procesa el Parquet unificado proveniento del paso s02."""
    func_logger = get_logger(f"{STEP_NAME}.process_structured")
    records: List[EmbeddingRecord] = []
    
    if struc_meta.get("status") == "completed" and struc_meta.get("silver_path"):
        silver_path = struc_meta["silver_path"]
        try:
            raw_bytes = st_account.read_file(directory=silver_path)
            df = pd.read_parquet(io.BytesIO(raw_bytes))
            content = df.to_csv(index=False)
            metadata = {
                "file_type": "parquet_merged",
                "rows": len(df),
                "column_count": len(df.columns),
                "silver_path": silver_path
            }
            records.append(_build_record(
                origin="structured_merged_dataset",
                content=content,
                metadata_dict=metadata,
                source_step="s02",
            ))
            func_logger.info(f"✅ Estructurado Normalizado DataFrame (s02) ({len(df)} filas)")
        except Exception as e:
            func_logger.error(f"❌ Error procesando el estructurado desde ADLS ({silver_path}): {e}")

    return records

def process_tabular_metadata(
    tabular_meta: Dict[str, Any], 
    st_account: DBFSMountPoint
) -> List[EmbeddingRecord]:
    """Carga los CSV limpios depositados por el s03 en silver dict."""
    func_logger = get_logger(f"{STEP_NAME}.process_tabular")
    records: List[EmbeddingRecord] = []

    for origin_path, entry in tabular_meta.items():
        if entry.get("status") != "completed" or not entry.get("silver_path"):
            continue
            
        silver_path = entry["silver_path"]
        try:
            raw_bytes = st_account.read_file(directory=silver_path)
            # Volvemos a leer de memoria, parseamos, y construimos un string para el embedder.
            df = pd.read_csv(io.BytesIO(raw_bytes), on_bad_lines="skip")
            content = df.to_csv(index=False)
            
            metadata = {
                "file_type": "csv_normalized",
                "rows": len(df),
                "column_count": len(df.columns),
                "silver_path": silver_path
            }
            records.append(_build_record(
                origin=origin_path,
                content=content,
                metadata_dict=metadata,
                source_step="s03",
            ))
            func_logger.info(f"✅ Tabular Normalizado (s03): origen {origin_path}")
        except Exception as e:
            func_logger.error(f"❌ Error procesando tabular normalizado {silver_path}: {e}")

    return records

def build_embedding_dataframe(*record_lists: List[EmbeddingRecord]) -> pd.DataFrame:
    """Fusiona múltiples listas de registros normalizados en un Frame único.
    Deduplicando logicamente."""
    func_logger = get_logger(f"{STEP_NAME}.build_embedding_dataframe")
    all_records: List[EmbeddingRecord] = []
    
    for record_list in record_lists:
        all_records.extend(record_list)

    if not all_records:
        func_logger.warning("⚠️ No hay registros normalizados listos para consolidar.")
        return pd.DataFrame(columns=["id", "origin", "content", "metadata", "update_at"])

    df = pd.DataFrame([asdict(r) for r in all_records])

    # Resolucion de conflictos: Conservar las corridas mas recientes.
    before = len(df)
    df = df.sort_values("update_at", ascending=False).drop_duplicates(subset="origin", keep="first")
    df = df.sort_values("origin").reset_index(drop=True)

    dupes = before - len(df)
    if dupes > 0:
        func_logger.info(f"🔄 Se limpiaron {dupes} elementos duplicados en resolución de recencia.")

    func_logger.info(f"📊 Consolidado final generado con {len(df)} registros totales preparados para Indexed Upload.")
    return df

# COMMAND ----------
# =====================================================
#             F L U J O   E X E C U C I O N
# =====================================================

if __name__ == "__main__":
    configure_logging()
    logger.info("📡 Iniciando extracción universal de datos persistentes...")

    # Instanciamos el validador Cosmos DB.
    cos_client = get_cosmos_client()
    cos_db = get_cosmos_database(cos_client)
    cosmos = CosmosDB(db=cos_db, container_name=settings.azure_cosmos_container, doc_id=DOC_ID)

    # 1. Recuperamos toda la metadata unificada desde la base de datos de control
    document = run_coro(cosmos.get_metadata_by_id(doc_id=DOC_ID))
    if not document:
        logger.error(f"🔥 Error Crítico: No se pudo localizar la trazabilidad de {DOC_ID} en CosmosDB.")
        raise RuntimeError("Metadata not found in CosmosDB - pipeline sync failure.")

    layer_metadata = document.get(CONTAINER, {}).get(MEDALLION, {})

    # 2. Despachadores de cada tipo de procesamiento de archivo basados en los reportes de Cosmos
    ocr_meta = layer_metadata.get("ocr_results", {})
    struc_meta = layer_metadata.get("structured_results", {})
    tab_meta = layer_metadata.get("tabular_results", {})
    img_meta = layer_metadata.get("image_results", {})

    logger.info("📦 Mapeando cargas útiles desde ADLS a registros de Embedding...")
    
    # 3. Procesamientos independientes accediendo al filesystem Databricks/ADLS
    r_ocr    = process_extracted_json(ocr_meta, st_account, "s01")
    r_struct = process_structured_metadata(struc_meta, st_account)
    r_tab    = process_tabular_metadata(tab_meta, st_account)
    r_img    = process_extracted_json(img_meta, st_account, "s04")  # Las imagenes comparten el schema JSON extraído de OCR.

    # 4. Consolidacion final en Master Frame vectorizable
    final_df = build_embedding_dataframe(r_ocr, r_struct, r_tab, r_img)

    # 5. Exportacion al Datalake para que s06 o cualquier Vectorizador LLM lo consuma
    if not final_df.empty:
        parquet_output_path = "azpocdk/embedding_records.parquet"
        
        # Escribir frame a buffer en memoria primero
        buffer = io.BytesIO()
        final_df.to_parquet(buffer, index=False)
        
        # Utilizar nuestro manejador de conexiones DBFS compatible universal:
        st_account.write_file(directory=parquet_output_path, content=buffer.getvalue())
        
        logger.info(f"✅ Pipeline 's05' ejecutado exítosamente! Output Maestro empaquetado en: {parquet_output_path}")
    else:
        logger.error("❌ El DataFrame final quedo absolutamente vacio. Verifique los pasos de recoleccion...")
