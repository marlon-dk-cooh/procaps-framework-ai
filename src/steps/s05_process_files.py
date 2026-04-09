# Databricks notebook source
"""s05 — Procesamiento de archivos para embedding.

Consolida las salidas de los pasos upstream (s01–s04) en un único
DataFrame normalizado con las columnas:

    id · origin · content · metadata · update_at

Cada fila representa un documento procesado, listo para ser
vectorizado e indexado en s06.
"""

import json
import uuid
import os
import io
from datetime import datetime, timezone
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import pandas as pd

from src.core.models import AnalyzedDocument, EmbeddingRecord
from src.utils.app_logger import get_logger, configure_logging

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s05 - Procesamiento de archivos."
UUID_NAMESPACE = uuid.NAMESPACE_URL

def _build_record(
    origin: str,
    content: str,
    metadata_dict: Dict[str, Any],
    source_step: str,
) -> EmbeddingRecord:
    """Construye un ``EmbeddingRecord`` normalizado.

    Args:
        origin: Ruta original del archivo en el Data Lake.
        content: Texto extraído o datos serializados.
        metadata_dict: Diccionario de metadatos extensibles.
        source_step: Identificador del paso origen (e.g. ``"s01"``).

    Returns:
        ``EmbeddingRecord`` listo para insertarse en el DataFrame.
    """
    metadata_dict["source_step"] = source_step
    return EmbeddingRecord(
        id=str(uuid.uuid5(UUID_NAMESPACE, origin)),
        origin=origin,
        content=content,
        metadata=json.dumps(metadata_dict, ensure_ascii=False),
        update_at=datetime.now(timezone.utc).isoformat(),
    )

def process_ocr_results(
    results: Dict[str, Any],
) -> List[EmbeddingRecord]:
    """Convierte la salida de ``s01.process_with_ocr()`` en registros.

    Args:
        results: Diccionario ``{file_path: {path, analyzed_document, status, ...}}``.

    Returns:
        Lista de ``EmbeddingRecord`` para los documentos procesados exitosamente.
    """
    logger = get_logger(f"{STEP_NAME}.process_ocr_results")
    records: List[EmbeddingRecord] = []

    for file_path, entry in results.items():
        if entry.get("status") != "completed":
            logger.warning("⏭️ Saltando %s (status=%s)", file_path, entry.get("status"))
            continue

        doc: Optional[AnalyzedDocument] = entry.get("analyzed_document")
        if doc is None:
            logger.warning("⏭️ %s no tiene AnalyzedDocument.", file_path)
            continue

        metadata = {
            "file_type": doc.file_type,
            "pages": doc.pages,
            "paragraphs_count": len(doc.paragraphs),
            "tables_count": len(doc.tables),
            **doc.metadata,
        }
        records.append(_build_record(
            origin=file_path,
            content=doc.content,
            metadata_dict=metadata,
            source_step="s01",
        ))
        logger.info("✅ Registro creado para %s (%d chars)", file_path, len(doc.content))

    logger.info("📦 s01 → %d registros generados.", len(records))
    return records

def process_structured_files(
    parquet_buffers: List[io.BytesIO],
    parquet_paths: List[str],
    json_buffers: List[io.BytesIO],
    json_paths: List[str],
) -> List[EmbeddingRecord]:
    """Convierte la salida de ``s02`` (parquet y JSON cargados) en registros.

    Los archivos Parquet se deserializan como DataFrame y se serializan
    como CSV string.  Los JSON se decodifican directamente como texto.

    Args:
        parquet_buffers: Lista de ``BytesIO`` con contenido Parquet.
        parquet_paths: Rutas originales correspondientes a cada buffer Parquet.
        json_buffers: Lista de ``BytesIO`` con contenido JSON.
        json_paths: Rutas originales correspondientes a cada buffer JSON.

    Returns:
        Lista de ``EmbeddingRecord``.
    """
    logger = get_logger(f"{STEP_NAME}.process_structured_files")
    records: List[EmbeddingRecord] = []

    # --- Parquet ---
    for buf, path in zip(parquet_buffers, parquet_paths):
        try:
            buf.seek(0)
            df = pd.read_parquet(buf)
            content = df.to_csv(index=False)
            metadata = {
                "file_type": "parquet",
                "rows": len(df),
                "columns": list(df.columns),
            }
            records.append(_build_record(
                origin=path,
                content=content,
                metadata_dict=metadata,
                source_step="s02",
            ))
            logger.info("✅ Parquet %s → %d filas", path, len(df))
        except Exception as e:
            logger.error("❌ Error procesando Parquet %s: %s", path, e)

    # --- JSON ---
    for buf, path in zip(json_buffers, json_paths):
        try:
            buf.seek(0)
            raw = buf.read()
            content = raw.decode("utf-8")
            metadata = {
                "file_type": "json",
                "size_bytes": len(raw),
            }
            records.append(_build_record(
                origin=path,
                content=content,
                metadata_dict=metadata,
                source_step="s02",
            ))
            logger.info("✅ JSON %s → %d bytes", path, len(raw))
        except Exception as e:
            logger.error("❌ Error procesando JSON %s: %s", path, e)

    logger.info("📦 s02 → %d registros generados.", len(records))
    return records

def process_tabular_files(
    dataframes: Dict[str, pd.DataFrame],
) -> List[EmbeddingRecord]:
    """Convierte la salida de ``s03`` (DataFrames parseados) en registros.

    Cada DataFrame se serializa como CSV string completo.

    Args:
        dataframes: Diccionario ``{file_path: DataFrame}``
            generado por ``read_csv_files`` / pandas.

    Returns:
        Lista de ``EmbeddingRecord``.
    """
    logger = get_logger(f"{STEP_NAME}.process_tabular_files")
    records: List[EmbeddingRecord] = []

    for file_path, df in dataframes.items():
        if df is None:
            logger.warning("⏭️ DataFrame nulo para %s", file_path)
            continue

        try:
            content = df.to_csv(index=False)
            metadata = {
                "file_type": file_path.rsplit(".", 1)[-1].lower(),
                "rows": len(df),
                "columns": list(df.columns),
            }
            records.append(_build_record(
                origin=file_path,
                content=content,
                metadata_dict=metadata,
                source_step="s03",
            ))
            logger.info("✅ Tabular %s → %d filas, %d cols", file_path, len(df), len(df.columns))
        except Exception as e:
            logger.error("❌ Error procesando tabular %s: %s", file_path, e)

    logger.info("📦 s03 → %d registros generados.", len(records))
    return records


def process_image_results(
    results: Dict[str, AnalyzedDocument],
) -> List[EmbeddingRecord]:
    """Convierte la salida de ``s04.analyze_all_images()`` en registros.

    Args:
        results: Diccionario ``{file_path: AnalyzedDocument}``.

    Returns:
        Lista de ``EmbeddingRecord``.
    """
    logger = get_logger(f"{STEP_NAME}.process_image_results")
    records: List[EmbeddingRecord] = []

    for file_path, doc in results.items():
        metadata = {
            "file_type": doc.file_type,
            "pages": doc.pages,
            "paragraphs_count": len(doc.paragraphs),
            "tables_count": len(doc.tables),
            **doc.metadata,
        }
        records.append(_build_record(
            origin=file_path,
            content=doc.content,
            metadata_dict=metadata,
            source_step="s04",
        ))
        logger.info("✅ Imagen %s → %d chars", file_path, len(doc.content))

    logger.info("📦 s04 → %d registros generados.", len(records))
    return records

def build_embedding_dataframe(
    *record_lists: List[EmbeddingRecord],
) -> pd.DataFrame:
    """Fusiona múltiples listas de registros en un DataFrame único.

    Deduplica por ``origin``, conservando el registro con el ``update_at``
    más reciente en caso de colisión.

    Args:
        *record_lists: Listas variables de ``EmbeddingRecord`` (una por paso).

    Returns:
        ``pd.DataFrame`` con columnas ``[id, origin, content, metadata, update_at]``.
    """
    logger = get_logger(f"{STEP_NAME}.build_embedding_dataframe")
    all_records: List[EmbeddingRecord] = []
    for record_list in record_lists:
        all_records.extend(record_list)

    if not all_records:
        logger.warning("⚠️ No hay registros para construir el DataFrame.")
        return pd.DataFrame(columns=["id", "origin", "content", "metadata", "update_at"])

    df = pd.DataFrame([asdict(r) for r in all_records])

    # Deduplicar: mantener el registro con update_at más reciente por origin
    before = len(df)
    df = df.sort_values("update_at", ascending=False).drop_duplicates(subset="origin", keep="first")
    df = df.sort_values("origin").reset_index(drop=True)

    dupes = before - len(df)
    if dupes > 0:
        logger.info("🔄 %d duplicados eliminados por 'origin'.", dupes)

    logger.info(
        "📊 DataFrame construido: %d registros, columnas=%s",
        len(df), list(df.columns),
    )
    return df

def export_dataframe(
    df: pd.DataFrame,
    output_path: str = "./azpocdk",
    parquet_name: str = "embedding_records.parquet",
    csv_name: str = "embedding_records.csv",
) -> None:
    """Exporta el DataFrame a Parquet (primario) y CSV (inspección).

    Args:
        df: DataFrame con los registros de embedding.
        output_path: Directorio de salida.
        parquet_name: Nombre del archivo Parquet.
        csv_name: Nombre del archivo CSV.
    """
    logger = get_logger(f"{STEP_NAME}.export_dataframe")
    os.makedirs(output_path, exist_ok=True)

    parquet_path = os.path.join(output_path, parquet_name)
    csv_path = os.path.join(output_path, csv_name)

    df.to_parquet(parquet_path, index=False)
    logger.info("💾 Parquet exportado: %s (%d registros)", parquet_path, len(df))

    df.to_csv(csv_path, index=False)
    logger.info("💾 CSV exportado: %s (%d registros)", csv_path, len(df))
