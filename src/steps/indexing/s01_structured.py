"""
s01_structured.py
Structured ingestion step (JSON / Parquet / Delta).
"""

import argparse
import json
import os
import re
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple

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

from src.core.logging_config import get_logger
from src.core.pipeline_context import StepContext
from src.core.settings import load_config, load_manifest
from src.core.adls_uploader import get_uploader
from src.core.medallion_paths import indexing_source_list_prefix

logger = get_logger(__name__)

try:
    from openai import AzureOpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AzureOpenAI = None  # type: ignore

try:
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

STRUCTURED_EXTENSIONS = {".json", ".parquet", ".delta"}
MAX_SAMPLE_ROWS = 5
MAX_COLUMNS_TO_SHOW = 10
MIN_CONTENT_CHARS = 50
MIN_TEXT_LENGTH_FOR_RAG = 20
DEFAULT_SAMPLE_ROWS_FOR_LLM = 2


def _download_bytes_with_fallback(
    uploader,
    silver_client,
    staged_path: str,
    bronze_path: str,
) -> Tuple[bytes, str]:
    """
    Download structured file bytes preferring Silver staged_path.
    Falls back to Bronze file_path.
    Returns (bytes, source_label).
    """
    staged = (staged_path or "").strip().replace("\\", "/")
    if staged:
        try:
            staged_client = silver_client.get_file_client(staged)
            return staged_client.download_file().readall(), "silver_staging"
        except Exception as e:
            logger.warning(
                "Could not read staged_path from Silver (%s): %s. Falling back to Bronze.",
                staged,
                e,
            )

    bronze = (bronze_path or "").strip().replace("\\", "/")
    bronze_client = uploader.container_client.get_file_client(bronze)
    return bronze_client.download_file().readall(), "bronze"


def _load_latest_discovery_manifest(silver_client) -> List[Dict[str, Any]]:
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s00_discovery"), recursive=True
            )
        )
        manifest_paths = [
            p
            for p in paths
            if not p.is_directory and "discovery_manifest.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list discovery manifests: {e}")
        return []

    if not manifest_paths:
        return []

    latest_manifest = sorted(
        manifest_paths, key=lambda x: x.last_modified, reverse=True
    )[0]
    logger.info(f"Using discovery manifest: {latest_manifest.name}")
    try:
        file_client = silver_client.get_file_client(latest_manifest.name)
        data = json.loads(file_client.download_file().readall())
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Error reading manifest {latest_manifest.name}: {e}")
        return []


def _parse_json_file(file_path: str) -> List[Dict[str, Any]]:
    # Try JSON document first (array/object)
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        raw = f.read().strip()
    if not raw:
        return []
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [r for r in obj if isinstance(r, dict)]
        if isinstance(obj, dict):
            return [obj]
        return []
    except Exception:
        pass

    # Fallback: JSON lines
    rows: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            ln = line.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                continue
    return rows


def _parse_parquet_file(file_path: str) -> List[Dict[str, Any]]:
    if not PYARROW_AVAILABLE:
        raise ImportError("pyarrow is required for parquet ingestion")
    table = pq.read_table(file_path)
    return table.to_pylist()


def _parse_delta_file(file_path: str) -> List[Dict[str, Any]]:
    # ADLS Delta table parsing requires Spark Delta runtime; local file handling is best-effort.
    # In this workflow, discovery usually lists files. If extension is .delta file, we skip.
    logger.warning(
        "Delta parsing is not implemented for local-file mode: %s", file_path
    )
    return []


def _is_json_homogeneous(sample: List[Dict[str, Any]]) -> bool:
    if not sample:
        return False
    base_keys = set(sample[0].keys())
    for row in sample[1:]:
        if set(row.keys()) != base_keys:
            return False
    return True


def _infer_textual_columns(
    sample: List[Dict[str, Any]], columns: List[str]
) -> List[str]:
    """
    Detect columns with text useful for semantic retrieval.
    """
    selected: List[str] = []
    for col in columns:
        has_text = False
        for row in sample:
            val = row.get(col)
            if isinstance(val, str) and len(val.strip()) >= MIN_TEXT_LENGTH_FOR_RAG:
                has_text = True
                break
        if has_text:
            selected.append(col)
    return selected


def _get_llm_client(config: Dict[str, Any], llm_key: str) -> Optional[Any]:
    if not OPENAI_AVAILABLE:
        return None
    llm_cfg = config.get("llms", {}).get(llm_key, {})
    if not llm_cfg:
        return None
    endpoint = (llm_cfg.get("endpoint") or "").strip()
    api_key = (llm_cfg.get("api_key") or "").strip()
    api_version = (llm_cfg.get("api_version") or "2024-08-01-preview").strip()
    if not endpoint:
        return None
    try:
        if api_key:
            return AzureOpenAI(
                azure_endpoint=endpoint,
                api_key=api_key,
                api_version=api_version,
            )
        from src.core.secret_helper import get_openai_token_provider

        token_provider = get_openai_token_provider()
        return AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=token_provider,
            api_version=api_version,
        )
    except Exception as e:
        logger.warning("Could not create LLM client for structured inference: %s", e)
        return None


def _resolve_known_field_mapping(
    config: Dict[str, Any], file_name: str, file_ext: str
) -> Dict[str, Any]:
    """
    Resolve known content/metadata field mapping from config.
    Priority:
      1) per-file mapping
      2) per-extension mapping
      3) default mapping
    """
    section = config.get("json_file_structure", {}) or {}
    mappings = section.get("mappings", {}) or {}
    by_file = mappings.get("by_file", {}) or {}
    by_ext = mappings.get("by_extension", {}) or {}
    default_map = mappings.get("default", {}) or {}

    selected = {}
    if file_name in by_file and isinstance(by_file[file_name], dict):
        selected = by_file[file_name]
    elif file_ext in by_ext and isinstance(by_ext[file_ext], dict):
        selected = by_ext[file_ext]
    elif isinstance(default_map, dict):
        selected = default_map

    content_fields = selected.get("content_fields") or []
    metadata_fields = selected.get("metadata_fields") or []
    if isinstance(content_fields, str):
        content_fields = [content_fields]
    if isinstance(metadata_fields, str):
        metadata_fields = [metadata_fields]
    return {
        "content_fields": [str(x).strip() for x in content_fields if str(x).strip()],
        "metadata_fields": [str(x).strip() for x in metadata_fields if str(x).strip()],
        "source": "known" if content_fields else "unknown",
    }


def _infer_fields_with_llm(
    config: Dict[str, Any],
    file_name: str,
    file_ext: str,
    sample_rows: List[Dict[str, Any]],
    columns: List[str],
) -> Dict[str, Any]:
    """
    Infer content/metadata fields using LLM over a small sample.
    Returns empty content_fields on failure.
    """
    section = config.get("json_file_structure", {}) or {}
    llm_key = section.get("llm_key", "gpt4_reasoner")
    llm_client = _get_llm_client(config, llm_key)
    llm_cfg = config.get("llms", {}).get(llm_key, {})
    deployment_name = (llm_cfg.get("deployment_name") or "").strip()
    llm_temperature = llm_cfg.get("temperature", 0)
    if not llm_client or not deployment_name:
        return {
            "content_fields": [],
            "metadata_fields": [],
            "reason": "llm_unavailable",
        }

    sample_json = json.dumps(sample_rows, ensure_ascii=False, indent=2)
    columns_str = ", ".join(columns)
    prompt = (
        f"Archivo: {file_name} ({file_ext})\n"
        f"Columnas: {columns_str}\n\n"
        f"Muestra (2 registros):\n{sample_json}\n\n"
        "Devuelve SOLO JSON válido con esta forma:\n"
        '{"content_fields":["..."],"metadata_fields":["..."],"reason":"..."}\n'
        "Reglas:\n"
        "- content_fields: campos semánticos para chunk/embeddings.\n"
        "- metadata_fields: ids, fechas, códigos, estados, campos técnicos.\n"
        "- Si no hay contenido útil, content_fields = []."
    )

    request_kwargs: Dict[str, Any] = {
        "model": deployment_name,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300,
    }
    # Some model families only support default temperature values.
    if llm_temperature is not None:
        request_kwargs["temperature"] = float(llm_temperature)

    try:
        response = llm_client.chat.completions.create(**request_kwargs)
    except Exception as e:
        err_txt = str(e).lower()
        retry_kwargs = dict(request_kwargs)
        changed = False

        if "max_tokens" in err_txt and "max_completion_tokens" in err_txt:
            retry_kwargs.pop("max_tokens", None)
            retry_kwargs["max_completion_tokens"] = 300
            changed = True

        if "temperature" in err_txt and "supported" in err_txt:
            retry_kwargs.pop("temperature", None)
            changed = True

        if not changed:
            logger.warning("LLM field inference failed for %s: %s", file_name, e)
            return {"content_fields": [], "metadata_fields": [], "reason": "llm_error"}

        try:
            response = llm_client.chat.completions.create(**retry_kwargs)
        except Exception as retry_e:
            logger.warning(
                "LLM field inference retry failed for %s: %s", file_name, retry_e
            )
            return {"content_fields": [], "metadata_fields": [], "reason": "llm_error"}

    text = ""
    if response.choices:
        text = (response.choices[0].message.content or "").strip()
    if not text:
        return {"content_fields": [], "metadata_fields": [], "reason": "llm_empty"}

    # Extract first JSON object from response
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {"content_fields": [], "metadata_fields": [], "reason": "llm_non_json"}
    try:
        parsed = json.loads(m.group(0))
    except Exception:
        return {
            "content_fields": [],
            "metadata_fields": [],
            "reason": "llm_json_parse_error",
        }

    raw_content = parsed.get("content_fields") or []
    raw_meta = parsed.get("metadata_fields") or []
    if isinstance(raw_content, str):
        raw_content = [raw_content]
    if isinstance(raw_meta, str):
        raw_meta = [raw_meta]

    valid_content = [c for c in raw_content if c in columns]
    valid_meta = [c for c in raw_meta if c in columns]
    return {
        "content_fields": valid_content,
        "metadata_fields": valid_meta,
        "reason": parsed.get("reason", "llm_ok"),
    }


def _build_content(row: Dict[str, Any], content_fields: List[str]) -> str:
    parts: List[str] = []
    for field in content_fields:
        value = row.get(field)
        if value is None:
            continue
        txt = str(value).strip()
        if txt:
            parts.append(f"{field}: {txt}")
    return "\n\n".join(parts).strip()


def _process_structured_file(
    config: Dict[str, Any],
    local_path: str,
    source_path: str,
    file_name: str,
    file_ext: str,
    run_id: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Returns (rows_for_output, reason_message)
    """
    if file_ext == ".json":
        records = _parse_json_file(local_path)
    elif file_ext == ".parquet":
        records = _parse_parquet_file(local_path)
    elif file_ext == ".delta":
        records = _parse_delta_file(local_path)
    else:
        return [], f"Unsupported structured extension: {file_ext}"

    if not records:
        return [], "No records found"

    sample = records[:MAX_SAMPLE_ROWS]
    columns = list(sample[0].keys()) if sample and isinstance(sample[0], dict) else []
    if not columns:
        return [], "No columns detected"

    if file_ext == ".json" and not _is_json_homogeneous(sample):
        return [], "JSON structure is heterogeneous"

    # 1) Known field validator from config
    mapping = _resolve_known_field_mapping(
        config=config, file_name=file_name, file_ext=file_ext
    )
    content_fields = [f for f in mapping["content_fields"] if f in columns]
    metadata_fields = [f for f in mapping["metadata_fields"] if f in columns]
    mapping_source = mapping.get("source", "unknown")

    # 2) If unknown, infer from first N rows with LLM
    if not content_fields:
        llm_rows_n = int(
            (config.get("json_file_structure", {}) or {}).get(
                "sample_rows_for_llm", DEFAULT_SAMPLE_ROWS_FOR_LLM
            )
        )
        llm_sample = sample[: max(1, llm_rows_n)]
        llm_map = _infer_fields_with_llm(
            config=config,
            file_name=file_name,
            file_ext=file_ext,
            sample_rows=llm_sample,
            columns=columns,
        )
        content_fields = [f for f in llm_map.get("content_fields", []) if f in columns]
        metadata_fields = [
            f for f in llm_map.get("metadata_fields", []) if f in columns
        ]
        mapping_source = "llm"

    # 3) Final fallback heuristic if LLM has no result
    if not content_fields:
        content_fields = _infer_textual_columns(sample, columns)
        mapping_source = "heuristic"

    if not content_fields:
        shown_cols = ", ".join(columns[:MAX_COLUMNS_TO_SHOW])
        return [], f"No textual fields useful for RAG. Columns seen: {shown_cols}"

    if not metadata_fields:
        metadata_fields = [c for c in columns if c not in content_fields]

    out_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(records):
        if not isinstance(row, dict):
            continue
        content = _build_content(row, content_fields)
        if len(content) < MIN_CONTENT_CHARS:
            continue

        meta: Dict[str, Any] = {}
        for f in metadata_fields:
            v = row.get(f)
            if v is None:
                continue
            # keep metadata serializable and compact
            meta[f] = str(v)
        meta["_structure_analysis"] = json.dumps(
            {
                "content_fields": content_fields,
                "metadata_fields": metadata_fields,
                "rag_compatible": True,
                "field_mapping_source": mapping_source,
            },
            ensure_ascii=False,
        )

        out_rows.append(
            {
                "file_id": f"{file_name}_{idx}",
                "file_name": file_name,
                "file_type": file_ext.lstrip("."),
                "source_path": source_path,
                "content": content,
                "metadata": meta,
                "run_id": run_id,
            }
        )

    return out_rows, f"Processed rows: {len(out_rows)}"


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    uploader = get_uploader()
    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception as e:
            logger.warning(f"Auto-init of uploader failed: {e}")

    if not uploader.initialized:
        detail = (
            uploader.get_last_error() if hasattr(uploader, "get_last_error") else None
        )
        raise RuntimeError(
            "ADLS Uploader could not be initialized in s01_structured_ingestion. "
            "Check managed identity RBAC and ADLS container permissions."
            + (f" Details: {detail}" if detail else "")
        )

    silver_client = uploader.get_layer_client("silver")
    manifest_data = _load_latest_discovery_manifest(silver_client)
    if not manifest_data:
        logger.warning("No discovery manifest found or readable. Exiting.")
        return

    structured_files = []
    for item in manifest_data:
        ext = (item.get("extension") or "").lower()
        if item.get("group") == "structured" and ext in STRUCTURED_EXTENSIONS:
            structured_files.append(item)

    logger.info(f"Found {len(structured_files)} structured files to process.")
    sl.set_input_count(len(structured_files))

    all_rows: List[Dict[str, Any]] = []
    for item in structured_files:
        source_path = item.get("file_path", "")
        staged_path = item.get("staged_path", "")
        file_name = item.get("filename") or os.path.basename(source_path) or "unknown"
        ext = (item.get("extension") or "").lower()
        temp_local = None
        try:
            binary, read_from = _download_bytes_with_fallback(
                uploader=uploader,
                silver_client=silver_client,
                staged_path=staged_path,
                bronze_path=source_path,
            )

            temp_fd, temp_local = tempfile.mkstemp(
                suffix=ext or "", prefix="structured_"
            )
            os.close(temp_fd)
            with open(temp_local, "wb") as f:
                f.write(binary)

            rows, reason = _process_structured_file(
                config=step_ctx.config,
                local_path=temp_local,
                source_path=source_path,
                file_name=file_name,
                file_ext=ext,
                run_id=step_ctx.run_id,
            )
            if rows:
                for row in rows:
                    md = row.get("metadata")
                    if isinstance(md, dict):
                        md["read_from"] = read_from
                all_rows.extend(rows)
                sl.record_file(file_name, "success", group="structured")
                logger.info("%s -> %s", file_name, reason)
            else:
                sl.record_error(file_name, reason)
                logger.warning("%s skipped: %s", file_name, reason)

        except Exception as e:
            logger.error("Error processing structured file %s: %s", file_name, e)
            sl.record_error(file_name, str(e))
        finally:
            if temp_local and os.path.exists(temp_local):
                os.remove(temp_local)

    sl.set_output_count(len(all_rows))

    # Output artifact for downstream processing
    success = uploader.upload_json_data(
        json_data=all_rows,
        remote_path="structured_data.json",
        dir_path="ingestion/structured",
        layer="silver",
        source_step="s01_structured_ingestion",
    )
    if success:
        logger.info("Uploaded structured output rows: %s", len(all_rows))
    else:
        logger.error("Failed to upload structured output.")


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
        config,
        manifest,
        args.run_id,
        args.task_run_id,
        step_name="s01_structured_ingestion",
    )
    process(step_ctx)
