"""
s01_tabular.py
Tabular ingestion step (CSV / XLSX / XLS).
"""

import argparse
import json
import os
import sys
import tempfile
from typing import Any, Dict, List, Tuple

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

SUPPORTED_TABULAR_EXTENSIONS = {".csv", ".xlsx", ".xls"}

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def _download_bytes_with_fallback(
    uploader,
    silver_client,
    staged_path: str,
    bronze_path: str,
) -> Tuple[bytes, str]:
    """
    Download tabular file bytes preferring Silver staged_path.
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


def _load_tabular_file(file_path: str):
    if not PANDAS_AVAILABLE:
        raise ImportError("pandas is required for tabular ingestion")

    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    if ext in (".xlsx", ".xls"):
        # openpyxl is already included in job libraries
        return pd.read_excel(file_path, engine="openpyxl")
    raise ValueError(f"Unsupported tabular format: {ext}")


def _process_tabular_file(
    local_path: str, source_path: str, file_name: str, run_id: str
) -> Tuple[List[Dict[str, Any]], str]:
    try:
        df = _load_tabular_file(local_path)
    except Exception as e:
        return [], f"Failed to read tabular file: {e}"

    if "content" not in list(df.columns):
        return [], "Rejected: missing required 'content' column"

    rows: List[Dict[str, Any]] = []
    for row_idx, row in df.iterrows():
        content = row.get("content")
        if content is None or str(content).strip() == "":
            continue
        meta: Dict[str, Any] = {}
        for col in df.columns:
            if col == "content":
                continue
            value = row.get(col)
            if value is None:
                continue
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            meta[col] = str(value)

        rows.append(
            {
                "file_id": f"{file_name}_{row_idx}",
                "file_name": file_name,
                "file_type": os.path.splitext(file_name)[1].lstrip(".").lower(),
                "source_path": source_path,
                "content": str(content),
                "metadata": meta,
                "run_id": run_id,
            }
        )
    return rows, f"Processed rows: {len(rows)}"


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
            "ADLS Uploader could not be initialized in s01_tabular_ingestion. "
            "Check managed identity RBAC and ADLS container permissions."
            + (f" Details: {detail}" if detail else "")
        )

    silver_client = uploader.get_layer_client("silver")
    manifest_data = _load_latest_discovery_manifest(silver_client)
    if not manifest_data:
        logger.warning("No discovery manifest found or readable. Exiting.")
        return

    tabular_files = []
    for item in manifest_data:
        ext = (item.get("extension") or "").lower()
        if item.get("group") == "tabular" and ext in SUPPORTED_TABULAR_EXTENSIONS:
            tabular_files.append(item)

    logger.info(f"Found {len(tabular_files)} tabular files to process.")
    sl.set_input_count(len(tabular_files))

    all_rows: List[Dict[str, Any]] = []
    rejected = 0
    for item in tabular_files:
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
            temp_fd, temp_local = tempfile.mkstemp(suffix=ext or "", prefix="tabular_")
            os.close(temp_fd)
            with open(temp_local, "wb") as f:
                f.write(binary)

            rows, reason = _process_tabular_file(
                local_path=temp_local,
                source_path=source_path,
                file_name=file_name,
                run_id=step_ctx.run_id,
            )
            if rows:
                for row in rows:
                    md = row.get("metadata")
                    if isinstance(md, dict):
                        md["read_from"] = read_from
                all_rows.extend(rows)
                sl.record_file(file_name, "success", group="tabular")
                logger.info("%s -> %s", file_name, reason)
            else:
                rejected += 1
                sl.record_error(file_name, reason)
                logger.warning("%s rejected: %s", file_name, reason)

        except Exception as e:
            rejected += 1
            logger.error("Error processing tabular file %s: %s", file_name, e)
            sl.record_error(file_name, str(e))
        finally:
            if temp_local and os.path.exists(temp_local):
                os.remove(temp_local)

    sl.set_output_count(len(all_rows))
    logger.info(
        "Tabular processing completed. rows=%s rejected_files=%s",
        len(all_rows),
        rejected,
    )

    success = uploader.upload_json_data(
        json_data=all_rows,
        remote_path="tabular_data.json",
        dir_path="ingestion/tabular",
        layer="silver",
        source_step="s01_tabular_ingestion",
    )
    if success:
        logger.info("Uploaded tabular output rows: %s", len(all_rows))
    else:
        logger.error("Failed to upload tabular output.")


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
        step_name="s01_tabular_ingestion",
    )
    process(step_ctx)
