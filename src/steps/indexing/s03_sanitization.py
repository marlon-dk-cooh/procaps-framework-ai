import argparse
import os
import sys
import json

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
from src.core.sanitization import PIIFilter

logger = get_logger(__name__)


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Config
    pii_config = step_ctx.config.get("security", {}).get("pii", {})
    enabled = pii_config.get("enabled", False)
    provider = pii_config.get("provider", "regex")
    # Endpoint might be in config or need to be fetched via secrets if strictly mapped
    # Use generic endpoint key if not specific
    # Config keys were: "endpoint: ${...}"
    endpoint = pii_config.get("endpoint")

    if not enabled:
        logger.info("PII sanitization disabled in config.")
        # We still need to pass data through?
        # Yes, usually "sanitized_chunks" is expected by next step.
        # We'll just copy.

    processor = PIIFilter(provider=provider, endpoint=endpoint) if enabled else None

    uploader = get_uploader()
    # Init uploader logic (redacted for brevity, assumed shared/init)
    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception:
            pass

    # Read Input: chunks_data.json from Silver container
    silver_client = uploader.get_layer_client("silver")
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s02_chunking"), recursive=True
            )
        )
        file_paths = [
            p for p in paths if not p.is_directory and "chunks_data.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list paths: {e}")
        file_paths = []

    if not file_paths:
        logger.warning("No input data found for sanitization. Exiting.")
        return

    latest_file = sorted(file_paths, key=lambda x: x.last_modified, reverse=True)[0]
    logger.info(f"Using input file: {latest_file.name}")

    try:
        file_client = silver_client.get_file_client(latest_file.name)
        download = file_client.download_file()
        data = json.loads(download.readall())
    except Exception as e:
        logger.error(f"Could not read input file {latest_file.name}: {e}")
        return  # Exit or raise

    sanitized_results = []
    sl.set_input_count(len(data))

    unique_sources = set()

    for item in data:
        content = item.get("content", "")
        meta = item.get("metadata", {})

        orig_filename = (
            meta.get("original_metadata", {}).get("original_filename")
            or meta.get("source_doc_id")
            or "unknown"
        )
        if orig_filename != "unknown":
            unique_sources.add(orig_filename)

        if processor and content:
            content = processor.sanitize(content)

        new_item = item.copy()
        new_item["content"] = content
        new_item["metadata"]["sanitized"] = True
        new_item["metadata"]["source_step"] = "s03_sanitization"

        sanitized_results.append(new_item)

    for source in unique_sources:
        sl.record_file(source, "success")

    sl.set_output_count(len(sanitized_results))

    # Upload
    if sanitized_results:
        uploader.upload_json_data(
            json_data=sanitized_results,
            remote_path="sanitized_chunks.json",
            dir_path="processing/sanitized",
            layer="silver",
            source_step="s03_sanitization",
        )
        logger.info(f"Uploaded {len(sanitized_results)} sanitized chunks")


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
        config, manifest, args.run_id, args.task_run_id, step_name="s03_sanitization"
    )

    process(step_ctx)
