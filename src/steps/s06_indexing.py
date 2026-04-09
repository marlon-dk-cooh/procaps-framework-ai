# Databricks notebook source
"""s06 — Indexación en Azure AI Search.

Lee el DataFrame de embedding records producido por s05, genera
vectores de embedding con Azure OpenAI (text-embedding-3-large) y
sube los documentos al índice ``procaps-index`` en Azure AI Search.

Chunking: si el contenido excede el límite de tokens del modelo,
se divide en fragmentos con overlap y cada fragmento se indexa
como un documento independiente.
"""

import json
import math
from typing import Any, Dict, List

import pandas as pd
from openai import AzureOpenAI

from config.settings import settings
from src.infrastructure.connections import AzureSearchConnection
from src.utils.app_logger import get_logger, configure_logging

# ================ CARGA DE SETTINGS ==================
STEP_NAME = "s06 - Indexación en Azure AI Search."

# text-embedding-3-large: 3072 dims, 8191 tokens max
VECTOR_DIMENSIONS = 3072
MAX_TOKENS = 8191
# Heurística conservadora: ~4 chars/token → ~28,000 chars por chunk
MAX_CHARS_PER_CHUNK = 28_000
CHUNK_OVERLAP_CHARS = 500

# ===================Lógica principal==================

def load_metadata_blob(raw_metadata: Any) -> Dict[str, Any]:
    """Parse metadata defensively, falling back to an empty dict."""
    if isinstance(raw_metadata, dict):
        return raw_metadata
    if not isinstance(raw_metadata, str) or not raw_metadata.strip():
        return {}

    try:
        parsed = json.loads(raw_metadata)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def chunk_content(
    text: str,
    max_chars: int = MAX_CHARS_PER_CHUNK,
    overlap: int = CHUNK_OVERLAP_CHARS,
) -> List[str]:
    """Divide un texto largo en fragmentos con overlap.

    Si el texto cabe en un solo chunk, devuelve una lista de un
    elemento sin modificación.

    Args:
        text: Texto a dividir.
        max_chars: Tamaño máximo de cada chunk en caracteres.
        overlap: Caracteres de solapamiento entre chunks consecutivos.

    Returns:
        Lista de strings, cada uno dentro del límite de caracteres.
    """
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = start + max_chars
        chunks.append(text[start:end])
        start = end - overlap

    return chunks


def expand_chunks(df: pd.DataFrame) -> pd.DataFrame:
    """Expande filas cuyo contenido excede el límite en múltiples filas.

    Cada chunk recibe un ``id`` derivado: ``{original_id}_chunk_{n}``.
    El ``id`` original del documento padre se guarda en ``metadata``
    como ``parent_id`` y el número total de chunks como ``total_chunks``.

    Args:
        df: DataFrame con columnas ``[id, origin, content, metadata, update_at]``.

    Returns:
        DataFrame expandido: filas originales cortas se mantienen intactas,
        filas largas se reemplazan por sus chunks.
    """
    logger = get_logger(f"{STEP_NAME}.expand_chunks")
    rows: List[Dict[str, Any]] = []
    chunked_count = 0

    for _, row in df.iterrows():
        chunks = chunk_content(row["content"])

        if len(chunks) == 1:
            # Sin chunking, mantener la fila tal cual
            rows.append(row.to_dict())
        else:
            chunked_count += 1
            parent_id = row["id"]
            meta_base = load_metadata_blob(row.get("metadata"))

            for i, chunk_text in enumerate(chunks):
                chunk_meta = {
                    **meta_base,
                    "parent_id": parent_id,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                }
                rows.append({
                    "id": f"{parent_id}_chunk_{i}",
                    "origin": row["origin"],
                    "content": chunk_text,
                    "metadata": json.dumps(chunk_meta, ensure_ascii=False),
                    "update_at": row["update_at"],
                })

            logger.info(
                "✂️ %s → %d chunks (contenido: %d chars)",
                row["origin"], len(chunks), len(row["content"]),
            )

    result = pd.DataFrame(rows)
    logger.info(
        "📊 Expansión de chunks: %d filas → %d filas (%d documentos divididos)",
        len(df), len(result), chunked_count,
    )
    return result

def _get_embeddings_client() -> AzureOpenAI:
    """Crea un cliente de Azure OpenAI para embeddings."""
    return AzureOpenAI(
        azure_endpoint=settings.azure_openai_embeddings_endpoint,
        api_key=settings.azure_openai_key,
        api_version=settings.azure_openai_api_version,
    )

def generate_embeddings(
    texts: List[str],
    client: AzureOpenAI = None,
    model: str = None,
    batch_size: int = 16,
) -> List[List[float]]:
    """Genera vectores de embedding para una lista de textos.

    Envía en batches para evitar exceder los límites de la API.

    Args:
        texts: Lista de strings a embeber.
        client: Cliente de Azure OpenAI (se crea si es None).
        model: Nombre del deployment de embeddings.
        batch_size: Textos por solicitud a la API.

    Returns:
        Lista de vectores (cada uno una lista de floats).
    """
    logger = get_logger(f"{STEP_NAME}.generate_embeddings")

    if client is None:
        client = _get_embeddings_client()
    if model is None:
        model = settings.azure_openai_embeddings_model

    all_embeddings: List[List[float]] = []
    total = len(texts)
    total_batches = math.ceil(total / batch_size)

    for i in range(0, total, batch_size):
        batch = texts[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        logger.info(
            "🧠 Generando embeddings batch %d/%d (%d textos)...",
            batch_num, total_batches, len(batch),
        )

        response = client.embeddings.create(
            model=model,
            input=batch,
        )

        # Los resultados vienen ordenados por index
        batch_embeddings = [item.embedding for item in response.data]
        all_embeddings.extend(batch_embeddings)

        logger.info(
            "✅ Batch %d/%d completado (%d vectores, %d dims cada uno).",
            batch_num, total_batches, len(batch_embeddings),
            len(batch_embeddings[0]) if batch_embeddings else 0,
        )

    logger.info("📦 Total embeddings generados: %d", len(all_embeddings))
    return all_embeddings


# =====================================================
# Document preparation
# =====================================================

def prepare_search_documents(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convierte el DataFrame (con contentVector) al formato de Azure Search.

    Args:
        df: DataFrame con columnas ``[id, origin, content, metadata, update_at, contentVector]``.

    Returns:
        Lista de diccionarios, uno por documento, listos para ``merge_or_upload_documents``.
    """
    documents: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        documents.append({
            "id": row["id"],
            "origin": row["origin"],
            "content": row["content"],
            "metadata": row["metadata"],
            "update_at": row["update_at"],
            "contentVector": row["contentVector"],
        })
    return documents


# =====================================================
# Orchestrator
# =====================================================

def index_documents(
    df: pd.DataFrame,
    vector_dimensions: int = VECTOR_DIMENSIONS,
) -> Dict[str, Any]:
    """Orquesta el flujo completo de indexación.

    1. Expande chunks si el contenido excede el límite de tokens.
    2. Crea o actualiza el índice en Azure AI Search.
    3. Genera embeddings para cada documento.
    4. Sube los documentos al índice.

    Args:
        df: DataFrame de s05 con columnas ``[id, origin, content, metadata, update_at]``.
        vector_dimensions: Dimensiones del modelo de embedding.

    Returns:
        Resumen del upload ``{succeeded, failed, errors}``.
    """
    logger = get_logger(f"{STEP_NAME}.index_documents")
    logger.info("--- Iniciando indexación: %d documentos ---", len(df))

    # 1. Chunking
    df_expanded = expand_chunks(df)

    # 2. Crear/actualizar índice
    search_conn = AzureSearchConnection(
        endpoint=settings.azure_search_endpoint,
        key=settings.azure_search_key,
        index_name=settings.azure_search_index,
    )
    search_conn.create_or_update_index(vector_dimensions=vector_dimensions)

    # 3. Generar embeddings
    texts = df_expanded["content"].tolist()
    embeddings = generate_embeddings(texts)
    df_expanded = df_expanded.copy()
    df_expanded["contentVector"] = embeddings

    # 4. Preparar y subir documentos
    documents = prepare_search_documents(df_expanded)
    summary = search_conn.upload_documents(documents)

    logger.info("--- Indexación finalizada ---")
    return summary

if __name__ == "__main__":
    configure_logging()
    logger = get_logger(STEP_NAME)
    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")

    # 1. Leer DataFrame producido por s05
    input_path = "./azpocdk/embedding_records.parquet"
    logger.info("📂 Leyendo DataFrame desde %s", input_path)
    df = pd.read_parquet(input_path)
    logger.info("📊 DataFrame cargado: %d filas, columnas=%s", len(df), list(df.columns))

    # 2. Ejecutar indexación completa
    summary = index_documents(df)

    # 3. Resumen final
    logger.info(
        "📋 Resumen: %d exitosos, %d fallidos",
        summary["succeeded"], summary["failed"],
    )
    if summary["errors"]:
        for err in summary["errors"]:
            logger.error("  ❌ %s", err)

    logger.info(f"--- Paso {STEP_NAME} finalizado ---")
