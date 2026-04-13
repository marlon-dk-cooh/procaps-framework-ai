import argparse
import os
import sys
import json
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
from src.core.medallion_paths import indexing_source_list_prefix
from src.core.extraction import FileExtractor
from src.core.ocr_client import OCRClient

logger = get_logger(__name__)


def _download_bytes_with_fallback(
    uploader,
    silver_client,
    staged_path: str,
    bronze_path: str,
):
    """
    Download document bytes preferring Silver staged_path.
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


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Config keys
    ocr_endpoint = step_ctx.config.get("ocr", {}).get("endpoint")
    ocr_key = step_ctx.config.get("ocr", {}).get("api_key")

    # Initialize services
    ocr_client = OCRClient(endpoint=ocr_endpoint, key=ocr_key)
    # extractor = FileExtractor(ocr_client=ocr_client) # Replaced by IRNormalizer
    from src.core.ir_normalizer import IRNormalizer

    # We still need extractor instance to pass to normalizer if we want to share OCR client
    extractor = FileExtractor(ocr_client=ocr_client)
    normalizer = IRNormalizer(file_extractor=extractor)

    uploader = get_uploader()

    # Ensure uploader initialized (for reading too, reuse logic)
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
            "ADLS Uploader could not be initialized. "
            "Check managed identity RBAC and ADLS container permissions."
            + (f" Details: {detail}" if detail else "")
        )

    # Find the latest discovery manifest (read from Silver container)
    logger.info("Looking for latest discovery manifest...")
    silver_client = uploader.get_layer_client("silver")
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
        logger.error(f"Failed to list paths: {e}")
        manifest_paths = []

    if not manifest_paths:
        logger.warning(
            "No discovery manifest found in Silver (%s). Exiting.",
            indexing_source_list_prefix("s00_discovery"),
        )
        return

    # Sort by last_modified to get the latest manifest
    latest_manifest = sorted(
        manifest_paths, key=lambda x: x.last_modified, reverse=True
    )[0]
    logger.info(f"Using manifest: {latest_manifest.name}")

    try:
        file_client = silver_client.get_file_client(latest_manifest.name)
        manifest_data = json.loads(file_client.download_file().readall())
    except Exception as e:
        logger.error(f"Error reading manifest {latest_manifest.name}: {e}")
        return

    textual_files = [item for item in manifest_data if item.get("group") == "textual"]
    logger.info(f"Found {len(textual_files)} textual files to process.")
    sl.set_input_count(len(textual_files))

    results = []
    from dataclasses import asdict

    now = datetime.now()

    for item in textual_files:
        file_path = item["file_path"]
        staged_path = item.get("staged_path", "")
        filename = item["filename"]
        logger.info(f"Processing: {filename}")

        try:
            # Download to temp
            content, read_from = _download_bytes_with_fallback(
                uploader=uploader,
                silver_client=silver_client,
                staged_path=staged_path,
                bronze_path=file_path,
            )

            # Use safe filename for temp storage
            import tempfile

            temp_fd, temp_local = tempfile.mkstemp(
                suffix=item.get("extension", ""), prefix="tmp_"
            )
            os.close(temp_fd)  # Close it so we can write traditionally

            with open(temp_local, "wb") as f:
                f.write(content)

            # Normalize to IR
            ir = normalizer.normalize(temp_local)

            # Clean up
            if os.path.exists(temp_local):
                os.remove(temp_local)

            if ir:
                # Add extra source metadata that Normalizer might not know
                ir.metadata["original_filename"] = filename
                ir.metadata["source_path"] = file_path
                ir.metadata["read_from"] = read_from
                ir.metadata["ingestion_date"] = now.isoformat()

                results.append(asdict(ir))
                sl.record_file(filename, "success", group="textual")
            else:
                sl.record_error(filename, "IR Normalizer returned None")

        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            sl.record_error(filename, str(e))
            if "temp_local" in locals() and os.path.exists(temp_local):
                os.remove(temp_local)
            # Continue to next file

    sl.set_output_count(len(results))

    # Upload results to Silver (IR objects)
    if results:
        remote_out = "ir_data.json"  # Renamed from textual_data.json to reflect IR
        # We upload to 'silver' layer.
        success = uploader.upload_json_data(
            json_data=results,
            remote_path=remote_out,
            dir_path="ingestion/ir",
            layer="silver",
            source_step="s01_textual",
        )
        if success:
            logger.info("Successfully uploaded IR results to Silver")
        else:
            logger.error("Failed to upload results")
    else:
        logger.warning("No results to upload")


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
        step_name="s01_textual_ingestion",
    )

    process(step_ctx)
