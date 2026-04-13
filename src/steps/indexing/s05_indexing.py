import argparse
import os
import sys
import json
import hashlib
from typing import Any, Dict, List

# -- Databricks path bootstrap --------------------------------------------------
# spark_python_task may execute files via exec(), which can omit __file__.
import inspect

_script_file = globals().get("__file__")
if not _script_file:
    _frame = inspect.currentframe()
    _script_file = _frame.f_code.co_filename if _frame else ""

_candidates = []
if _script_file:
    _script_dir = os.path.dirname(os.path.abspath(_script_file))
    _candidates.append(os.path.abspath(os.path.join(_script_dir, "../../..")))

_cwd = os.path.abspath(os.getcwd())
_candidates.extend(
    [
        _cwd,
        os.path.abspath(os.path.join(_cwd, "..")),
        os.path.abspath(os.path.join(_cwd, "../..")),
    ]
)

for _proj in _candidates:
    if os.path.isdir(os.path.join(_proj, "src")):
        if _proj not in sys.path:
            sys.path.insert(0, _proj)
        break
# -------------------------------------------------------------------------------

from src.core.settings import load_config, load_manifest
from src.core.pipeline_context import StepContext
from src.core.logging_config import get_logger
from src.core.adls_uploader import get_uploader
from src.core.medallion_paths import indexing_source_list_prefix
from src.core.search_client import SearchClient

logger = get_logger(__name__)


# def _estimate_token_count(text: str) -> int:
#     if not text:
#         return 0
#     # Approximation kept lightweight for indexing metadata.
#     return len(text.split())


def _extract_keywords(meta: Dict[str, Any]) -> List[str]:
    """Keywords from metadata; typically generated in s04_embeddings_keywords and read from embeddings_data.json."""
    original = meta.get("original_metadata", {}) if isinstance(meta, dict) else {}
    candidates = (
        meta.get("keywords") or original.get("keywords") or original.get("tags") or []
    )
    if isinstance(candidates, str):
        candidates = [k.strip() for k in candidates.split(",") if k.strip()]
    if not isinstance(candidates, list):
        return []
    return [str(k) for k in candidates if k]


def _source_display_value(meta: Dict[str, Any]) -> str:
    """Prefer document name (original_filename) for search index 'source' field."""
    original = meta.get("original_metadata", {}) if isinstance(meta, dict) else {}
    return (
        (original.get("original_filename") or "").strip()
        or (meta.get("original_filename") or "").strip()
        or (original.get("source_path") or "").strip()
        or (meta.get("source_path") or "").strip()
        or (meta.get("source_doc_id") or "").strip()
        or (meta.get("source") or "").strip()
        or "unknown"
    ) or "unknown"


def _type_source_display_value(meta: Dict[str, Any]) -> str:
    """Resolve document type from mime_type and optionally normalize to short label (e.g. PDF, DOCX, PPTX, etc.)."""
    original = meta.get("original_metadata", {}) if isinstance(meta, dict) else {}
    mime = (
        (meta.get("type_source") or "").strip()
        or (original.get("mime_type") or "").strip()
        or "unknown"
    )
    if not mime or mime == "unknown":
        return "unknown"
    # Short labels for common types (search/filter friendly)
    _LABELS = {
        "application/pdf": "pdf",
        "text/plain": "txt",
        "text/markdown": "markdown",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/msword": "doc",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
        "application/vnd.ms-powerpoint": "ppt",
        "application/json": "json",
    }
    return _LABELS.get(mime, mime)


def _stable_doc_id(item: Dict[str, Any]) -> str:
    """
    Build a deterministic document ID to keep indexing idempotent
    and enable obsolete-chunk cleanup across runs.
    """
    meta = item.get("metadata", {}) if isinstance(item, dict) else {}
    original = meta.get("original_metadata", {}) if isinstance(meta, dict) else {}
    content = (item.get("content") or "").strip() if isinstance(item, dict) else ""

    source_doc_id = (
        (meta.get("source_doc_id") or "").strip()
        or (original.get("source_path") or "").strip()
        or (original.get("original_filename") or "").strip()
        or "unknown-source"
    )
    chunk_ordinal = str(meta.get("chunk_ordinal", ""))
    strategy = (meta.get("strategy") or "").strip() or "unknown-strategy"
    content_fingerprint = hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
    raw = f"{source_doc_id}|{chunk_ordinal}|{strategy}|{content_fingerprint}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Config
    # vector_stores: vectordb_text
    vs_config = step_ctx.config.get("vector_stores", {}).get("vectordb_text", {})
    endpoint = vs_config.get("endpoint")
    key = vs_config.get("api_key")
    index_name = vs_config.get("index_name")

    if not endpoint or not index_name:
        raise ValueError("Missing search endpoint/index_name in config")

    vector_dimensions = vs_config.get("vector_dimensions", 1536)

    # Auto-create the index if it doesn't exist
    from src.core.search_index_manager import ensure_index_exists

    ensure_index_exists(
        endpoint=endpoint,
        index_name=index_name,
        vector_dimensions=vector_dimensions,
        key=key,
    )

    search_client = SearchClient(endpoint=endpoint, index_name=index_name, key=key)

    uploader = get_uploader()
    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception:
            pass

    # Read Input: embeddings_data.json from s04 (Silver container)
    silver_client = uploader.get_layer_client("silver")
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s04_embeddings_keywords"),
                recursive=True,
            )
        )
        file_paths = [
            p for p in paths if not p.is_directory and "embeddings_data.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list paths: {e}")
        file_paths = []

    if not file_paths:
        logger.warning("No input data found for indexing. Exiting.")
        return

    latest_file = sorted(file_paths, key=lambda x: x.last_modified, reverse=True)[0]
    logger.info(f"Using input file: {latest_file.name}")

    try:
        file_client = silver_client.get_file_client(latest_file.name)
        download = file_client.download_file()
        data = json.loads(download.readall())
    except Exception as e:
        logger.error(f"Could not read input {latest_file.name}: {e}")
        return

    sl.set_input_count(len(data))

    # Prepare logic
    # Reference schema needs: id, content, content_vector, metadata fields...
    # The 'chunk' metadata: count_tokens, count_characters, source, type_source, chunk_index, etc.
    # We flatten metadata for search index usually.

    search_docs = []
    unique_sources = set()

    for item in data:
        content = item.get("content")
        vector = item.get("vector")
        meta = item.get("metadata", {})

        orig_filename = (
            meta.get("original_metadata", {}).get("original_filename")
            or meta.get("source_doc_id")
            or "unknown"
        )
        if orig_filename != "unknown":
            unique_sources.add(orig_filename)

        if not content or not vector:
            continue

        # Deterministic ID allows idempotent updates and obsolete cleanup.
        doc_id = _stable_doc_id(item)

        # source = document name (original_filename); type_source = document type (e.g. PDF, DOCX)
        source_value = _source_display_value(meta)
        type_source_value = _type_source_display_value(meta)
        keywords = _extract_keywords(meta)

        doc = {
            "id": doc_id,
            "content": content,
            "content_vector": vector,
            "metadata": {
                "count_characters": len(content),
                # "count_tokens": _estimate_token_count(content),
                "source": source_value,
                "type_source": type_source_value,
            },
            "keywords": keywords,
        }
        # Add dynamic metadata fields if index supports them (Retrievable=True)
        # We assume strict schema for now.

        search_docs.append(doc)

    for source in unique_sources:
        sl.record_file(source, "success")

    sl.set_output_count(len(search_docs))

    # Upload documents to Azure AI Search
    if search_docs:
        # List existing document IDs in the index
        existing_ids = set(search_client.list_document_ids())

        # Get new document IDs
        new_ids = {doc["id"] for doc in search_docs}

        # Upload documents to Azure AI Search
        count = search_client.upload_documents(search_docs)

        logger.info(f"Indexed {count} documents successfully.")

        # Delete obsolete documents
        obsolete_ids = existing_ids - new_ids

        # Delete obsolete documents
        if obsolete_ids:
            deleted = search_client.delete_documents_by_ids(list(obsolete_ids))
            logger.info(f"Deleted {deleted} obsolete documents.")
        else:
            logger.info("No obsolete documents to delete.")
    else:
        logger.warning("No documents to index.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--task_run_id", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    manifest = load_manifest(args.manifest)
    step_ctx = StepContext(
        config, manifest, args.run_id, args.task_run_id, step_name="s05_indexing"
    )

    process(step_ctx)
