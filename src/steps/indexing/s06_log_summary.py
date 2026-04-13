import argparse
import os
import sys
import json
from datetime import datetime
from typing import Dict, Any, List

# -- Databricks path bootstrap --------------------------------------------------
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
from src.core.colombian_time import current_colombian_time
from src.core.medallion_paths import INDEXING_LOGS_ROOT

logger = get_logger(__name__)


def _build_sharepoint_indexing_summary(
    step_logs: List[Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Resume el paso s00_sharepoint desde su log en Silver (step_metadata) y flags de config.
    """
    idx_cfg = config.get("indexing") or {}
    sp_cfg = config.get("sharepoint") or {}
    out: Dict[str, Any] = {
        "step_log_found": False,
        "indexing_sharepoint_sync_enabled_config": idx_cfg.get(
            "sharepoint_sync_enabled"
        ),
        "sharepoint_enabled_config": sp_cfg.get("enabled"),
        "sync_ran": False,
        "sync_disabled_reason": None,
        "bronze_prefix": None,
        "documents_synced": 0,
        "skipped_unchanged": 0,
        "skipped_since_date": 0,
        "failed": 0,
    }
    for log in step_logs:
        if log.get("step_name") != "s00_sharepoint":
            continue
        out["step_log_found"] = True
        meta = log.get("step_metadata") or {}
        out["sync_ran"] = bool(meta.get("sync_ran"))
        out["sync_disabled_reason"] = meta.get("sync_disabled_reason")
        out["bronze_prefix"] = meta.get("bronze_prefix")
        for k in (
            "documents_synced",
            "skipped_unchanged",
            "skipped_since_date",
            "failed",
        ):
            if k in meta:
                try:
                    out[k] = int(meta[k])
                except (TypeError, ValueError):
                    out[k] = meta[k]
        break
    return out


def process(step_ctx: StepContext):
    """
    Log Summary Step:
    1. Read all JSON logs from silver/logs_indexing/<run_id>/
    2. Aggregate metrics, timestamps, costs, classification, and errors
    3. Write the final summary JSON to gold/logs_indexing/<run_id>/run_summary.json
    """
    logger.info(f"Starting Step: {step_ctx.step_name}")
    run_id = step_ctx.run_id

    uploader = get_uploader()
    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception as e:
            logger.error(f"Uploader initialization failed: {e}")
            return

    silver_logs_path = f"{INDEXING_LOGS_ROOT}/{run_id}"
    silver_client = uploader.service_client.get_file_system_client("silver")

    try:
        paths = list(silver_client.get_paths(path=silver_logs_path, recursive=False))
    except Exception as e:
        logger.error(f"Could not list paths in {silver_logs_path}: {e}")
        return

    log_files = [p for p in paths if not p.is_directory and p.name.endswith(".json")]
    logger.info(f"Found {len(log_files)} step logs in {silver_logs_path}")

    if not log_files:
        logger.warning("No step logs found. Cannot generate summary.")
        return

    step_logs = []
    for p in log_files:
        try:
            file_client = silver_client.get_file_client(p.name)
            download = file_client.download_file()
            data = json.loads(download.readall())
            step_logs.append(data)
        except Exception as e:
            logger.error(f"Failed to read log {p.name}: {e}")

    if not step_logs:
        logger.error("Failed to parse any step logs.")
        return

    # Aggregate Data
    start_times = []
    end_times = []
    all_errors = []
    overall_status = "success"

    metrics = {
        "files_discovered": 0,
        "files_processed": 0,  # total success in s01
        "files_failed": 0,  # total errors everywhere or just specific
        "chunks_generated": 0,
        "documents_indexed": 0,
        "sharepoint_documents_synced": 0,
    }
    classification = {}
    costs = {"openai_tokens_total": 0, "ocr_pages": 0}
    chunking_used_counts = {}

    for log in step_logs:
        step_name = log.get("step_name", "unknown")
        status = log.get("status", "success")

        if status == "failure":
            overall_status = "failure"
        elif status == "partial_success" and overall_status != "failure":
            overall_status = "partial_success"

        all_errors.extend(log.get("errors", []))

        start_ts = log.get("start_time")
        if start_ts:
            start_times.append(datetime.fromisoformat(start_ts))

        end_ts = log.get("end_time")
        if end_ts:
            end_times.append(datetime.fromisoformat(end_ts))

        costs["openai_tokens_total"] += log.get("costs", {}).get(
            "openai_tokens_total", 0
        )
        costs["ocr_pages"] += log.get("costs", {}).get("ocr_pages", 0)

        # Merge classifications (usually best from s00_discovery)
        for k, v in log.get("classification", {}).items():
            classification[k] = classification.get(k, 0) + v

        # Specific metrics
        if step_name == "s00_discovery":
            metrics["files_discovered"] = log.get("output_count", 0)
        elif step_name.startswith("s01_"):
            for file_entry in log.get("files_processed", []):
                if file_entry.get("status") == "success":
                    metrics["files_processed"] += 1
                else:
                    metrics["files_failed"] += 1
        elif step_name == "s02_chunking":
            metrics["chunks_generated"] = log.get("output_count", 0)
            # s02_chunking now records strategy in StepLogger classification via `group`.
            step_chunking_counts = log.get("classification", {}) or {}
            for strategy_name, count in step_chunking_counts.items():
                chunking_used_counts[strategy_name] = (
                    chunking_used_counts.get(strategy_name, 0) + count
                )
        elif step_name == "s05_indexing":
            metrics["documents_indexed"] = log.get("output_count", 0)

    sharepoint_indexing = _build_sharepoint_indexing_summary(
        step_logs, step_ctx.config
    )
    metrics["sharepoint_documents_synced"] = int(
        sharepoint_indexing.get("documents_synced") or 0
    )

    # Calculate overall duration
    global_start = min(start_times) if start_times else current_colombian_time()
    global_end = max(end_times) if end_times else current_colombian_time()
    duration = (global_end - global_start).total_seconds()

    # Extract config details
    pipeline_conf = step_ctx.config.get("pipeline", {})
    initiative_name = pipeline_conf.get("initiative_name", "Framework_LLM")
    env = pipeline_conf.get("environment", "dll")

    search_service = step_ctx.config.get("azure", {}).get("search_service", "unknown")
    index_name = (
        step_ctx.config.get("vector_stores", {})
        .get("vectordb_text", {})
        .get("index_name", "unknown")
    )
    chunking_conf = step_ctx.config.get("processing", {}).get("chunking", {})
    # Nombre de parámetro en config para seleccionar el tipo de chunking: type_chunking
    type_chunking = chunking_conf.get("type_chunking", "recursive")
    chunking_overrides = chunking_conf.get("strategy_overrides", {})
    chunking_strategies_configured = sorted(
        {type_chunking, *chunking_overrides.values()}
    )
    chunking_strategies_used = sorted(chunking_used_counts.keys())
    chunking_strategy_in_execution = (
        chunking_strategies_used[0]
        if len(chunking_strategies_used) == 1
        else chunking_strategies_used
    )

    chunking_parameters_configured = {
        "chunk_size": chunking_conf.get("chunk_size"),
        "chunk_overlap": chunking_conf.get("chunk_overlap"),
        "fixed_word": {
            "chunk_size_words": chunking_conf.get("chunk_size_words"),
            "chunk_overlap_words": chunking_conf.get("chunk_overlap_words"),
            "min_words": chunking_conf.get("min_words"),
            "strip_whitespace": chunking_conf.get("strip_whitespace"),
            "normalize_spaces": chunking_conf.get("normalize_spaces"),
        },
        "character": {
            "size": chunking_conf.get("size"),
            "overlap": chunking_conf.get("overlap"),
            "separator": chunking_conf.get("separator"),
            "is_separator_regex": chunking_conf.get("is_separator_regex"),
        },
        "recursive": {
            "separators": chunking_conf.get("separators"),
            "keep_separator": chunking_conf.get("keep_separator"),
            "recursive_is_separator_regex": chunking_conf.get(
                "recursive_is_separator_regex"
            ),
        },
        "markdown": {
            "markdown_headers": chunking_conf.get("markdown_headers"),
        },
        "semantic": {
            "semantic_threshold_type": chunking_conf.get("semantic_threshold_type"),
            "semantic_breakpoint_threshold_amount": chunking_conf.get(
                "semantic_breakpoint_threshold_amount"
            ),
            "semantic_embedding_model": chunking_conf.get("semantic_embedding_model"),
        },
    }

    # Solo reportar tipo de chunking usado y sus parametros asociados.
    def _get_chunking_parameters_for_type(type_name: str) -> Dict[str, Any]:
        type_params = chunking_parameters_configured.get(type_name) or {}
        if not isinstance(type_params, dict):
            type_params = {}
        return {
            "chunk_size": chunking_parameters_configured.get("chunk_size"),
            "chunk_overlap": chunking_parameters_configured.get("chunk_overlap"),
            **type_params,
        }

    if isinstance(chunking_strategy_in_execution, list):
        chunking_parameters_used = {
            t: _get_chunking_parameters_for_type(t)
            for t in chunking_strategy_in_execution
        }
    else:
        chunking_parameters_used = _get_chunking_parameters_for_type(
            chunking_strategy_in_execution
        )

    logger.info(
        "Chunking summary | default=%s | overrides=%s | configured=%s | used=%s",
        type_chunking,
        chunking_overrides,
        chunking_strategies_configured,
        chunking_strategies_used,
    )

    summary = {
        "project_name": initiative_name,
        "run_id": run_id,
        "pipeline": "databricks-index-pipeline",
        "environment": env,
        "timestamps": {
            "start": global_start.isoformat(),
            "end": global_end.isoformat(),
            "duration_seconds": duration,
        },
        "status": overall_status,
        "target": {"search_service": search_service, "index_name": index_name},
        "chunking": {
            "type_chunking": chunking_strategy_in_execution,
            "parameters": chunking_parameters_used,
        },
        "metrics": metrics,
        "sharepoint_indexing": sharepoint_indexing,
        "classification": classification,
        "costs": costs,
        "errors": all_errors,
        "silver_logs_processed": [p.name for p in log_files],
    }

    # Write to Gold
    gold_dir = f"{INDEXING_LOGS_ROOT}/{run_id}"
    gold_file = f"{gold_dir}/run_summary.json"
    gold_client = uploader.service_client.get_file_system_client("gold")

    try:
        uploader._ensure_directory_exists(gold_dir, gold_client)
        file_client = gold_client.get_file_client(gold_file)

        json_bytes = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")
        file_client.upload_data(json_bytes, overwrite=True)
        logger.info(f"Successfully wrote run summary to {gold_file}")
    except Exception as e:
        logger.error(f"Failed to write summary to gold: {e}")


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
        config, manifest, args.run_id, args.task_run_id, step_name="s06_log_summary"
    )
    process(step_ctx)
