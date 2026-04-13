import argparse
import json
import os
import sys
from typing import Tuple
from collections import defaultdict
from datetime import datetime

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
from src.core.colombian_time import current_colombian_time
from src.core.medallion_paths import INDEXING_STEPS_ROOT

logger = get_logger(__name__)

FILE_GROUPS = {
    "textual": [".pdf", ".docx", ".txt", ".pptx"],
    "tabular": [".csv", ".xlsx", ".xls"],
    "images": [".png", ".jpg", ".jpeg", ".tiff", ".webp", ".gif", ".bmp"],
    "structured": [".json", ".parquet", ".delta"],
}


def classify_extension(filename: str) -> Tuple[str, str]:
    """Return (group, extension) for a given filename."""
    base, ext = os.path.splitext(filename)
    ext = ext.lower()
    for group, extensions in FILE_GROUPS.items():
        if ext in extensions:
            return group, ext
    return "unknown", ext


def _partition_base_path(source_step: str, dir_path: str) -> str:
    """Build partitioned base path for Silver with source + date convention."""
    now = datetime.now()
    sub = dir_path.strip("/").replace("\\", "/")
    return (
        f"{INDEXING_STEPS_ROOT}/source={source_step}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{sub}"
    )


def _copy_file_to_silver_staging(
    uploader,
    silver_client,
    source_path: str,
    group: str,
    ext: str,
    filename: str,
) -> str:
    """
    Copy original file from Bronze to Silver staging grouped by type/extension.
    Returns staged file path in Silver.
    """
    ext_label = (ext or ".unknown").lstrip(".") or "unknown"
    base_dir = _partition_base_path("s00_discovery", f"staging/{group}/{ext_label}")
    staged_path = f"{base_dir}/{filename}".replace("\\", "/")

    # Ensure partition/group directory exists in Silver.
    uploader._ensure_directory_exists(base_dir, silver_client)  # pylint: disable=protected-access

    bronze_file_client = uploader.container_client.get_file_client(source_path)
    file_bytes = bronze_file_client.download_file().readall()

    silver_file_client = silver_client.get_file_client(staged_path)
    silver_file_client.upload_data(file_bytes, overwrite=True)
    return staged_path


def _upload_discovery_report(
    uploader,
    silver_client,
    report_text: str,
) -> str:
    """Upload a human-readable discovery report to Silver and return its path."""
    base_dir = _partition_base_path("s00_discovery", "discovery")
    report_path = f"{base_dir}/discovery_report.txt".replace("\\", "/")
    uploader._ensure_directory_exists(base_dir, silver_client)  # pylint: disable=protected-access
    silver_client.get_file_client(report_path).upload_data(
        report_text.encode("utf-8"), overwrite=True
    )
    return report_path


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    """
    Discovery Step:
    1. Scan origin ADLS container.
    2. Classify files to be processed.
    3. Upload `discovery_manifest.json` to Silver Layer.
    """
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Initialize uploader
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
            "ADLS Uploader could not be initialized in s00_discovery. "
            "Check managed identity RBAC on ADLS account/container."
            + (f" Details: {detail}" if detail else "")
        )

    silver_client = uploader.get_layer_client("silver")

    stats = defaultdict(int)
    discovered_files = []

    # Path prefix from manifest (e.g. "indexing" to scan only bronze/indexing/)
    raw_input = step_ctx.get_input("raw_documents") or {}
    path_prefix = (raw_input.get("path_prefix") or "").strip().rstrip("/")
    if path_prefix:
        path_prefix = f"{path_prefix}/"

    # Prefix combinations to ignore if they correspond to pipeline intermediate artifact folders
    # Usually ADLS uploader puts things in: bronze/, silver/, gold/
    ignore_prefixes = ("bronze/", "silver/", "gold/")

    scope_msg = f" under '{path_prefix.rstrip('/')}/'" if path_prefix else ""
    logger.info(f"Scanning for files in the ADLS 'bronze' container{scope_msg}...")

    # Scan via ADLS recursive get_paths (optionally scoped by path_prefix from manifest)
    paths = uploader.container_client.get_paths(path=path_prefix, recursive=True)

    input_cnt = 0
    for path_item in paths:
        if path_item.is_directory:
            continue

        input_cnt += 1
        file_path = path_item.name

        # Skip pipeline output directories if they reside in the same container
        if file_path.startswith(ignore_prefixes):
            stats["skipped_pipeline_artifact"] += 1
            # Do not record as error, just skipped
            continue

        filename = os.path.basename(file_path)
        group, ext = classify_extension(filename)

        if group == "unknown":
            stats["skipped_unknown"] += 1
            sl.record_error(filename, "Unknown file extension ignored")
            continue

        file_size = int(getattr(path_item, "content_length", 0) or 0)
        staged_path = ""
        try:
            staged_path = _copy_file_to_silver_staging(
                uploader=uploader,
                silver_client=silver_client,
                source_path=file_path,
                group=group,
                ext=ext,
                filename=filename,
            )
            stats["staged_copied"] += 1
        except Exception as copy_err:
            logger.error("Failed staging copy for %s: %s", file_path, copy_err)
            sl.record_error(filename, f"Staging copy failed: {copy_err}")
            stats["staged_failed"] += 1
            # Skip file from manifest if we can't stage it.
            continue

        discovered_files.append(
            {
                "file_path": file_path,
                "staged_path": staged_path,
                "filename": filename,
                "group": group,
                "extension": ext,
                "size_bytes": file_size,
                "discovered_at": current_colombian_time().isoformat(),
            }
        )

        stats[f"discovered_{group}"] += 1
        stats["total_size_bytes"] += file_size
        sl.record_file(filename, "success", group=group)

    sl.set_input_count(input_cnt)
    sl.set_output_count(len(discovered_files))

    logger.info(f"Discovery complete. Stats: {dict(stats)}")

    # Upload discovery report to Silver.
    report_lines = [
        "=== Discovery Report ===",
        f"generated_at_bogota: {current_colombian_time().isoformat()}",
        f"input_prefix: {path_prefix or '/'}",
        f"total_paths_scanned: {input_cnt}",
        f"files_discovered: {len(discovered_files)}",
        f"staged_copied: {stats.get('staged_copied', 0)}",
        f"staged_failed: {stats.get('staged_failed', 0)}",
        f"total_size_mb: {stats.get('total_size_bytes', 0) / (1024 * 1024):.2f}",
        "",
        "--- by_group ---",
    ]
    for g in ("textual", "tabular", "images", "structured"):
        report_lines.append(f"{g}: {stats.get(f'discovered_{g}', 0)}")
    report_lines.append("")
    report_lines.append("--- raw_stats_json ---")
    report_lines.append(json.dumps(dict(stats), ensure_ascii=False, indent=2))
    report_text = "\n".join(report_lines)
    report_path = _upload_discovery_report(
        uploader=uploader,
        silver_client=silver_client,
        report_text=report_text,
    )
    logger.info("Discovery report uploaded to Silver: %s", report_path)

    if discovered_files:
        remote_out = "discovery_manifest.json"

        success = uploader.upload_json_data(
            json_data=discovered_files,
            remote_path=remote_out,
            dir_path="discovery",
            layer="silver",
            source_step="s00_discovery",
        )
        if success:
            logger.info("Successfully uploaded discovery manifest to Silver")
        else:
            logger.error("Failed to upload discovery manifest")
    else:
        logger.warning("No files discovered to process.")


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
        config, manifest, args.run_id, args.task_run_id, step_name="s00_discovery"
    )

    process(step_ctx)
