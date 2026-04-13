"""
s01_visual.py
Visual ingestion step for image metadata extraction.
"""

import argparse
import os
import sys
import json
import tempfile
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

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
from src.core.ocr_client import OCRClient
from src.core.extraction import FileExtractor

logger = get_logger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif", ".webp"}

MIME_BY_EXTENSION = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".tiff": "image/tiff",
    ".bmp": "image/bmp",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

OCR_SUPPORTED_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "image/bmp",
    "image/tiff",
    "image/gif",
    "image/webp",
}


def _download_bytes_with_fallback(
    uploader, silver_client, staged_path: str, bronze_path: str
):
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


def _extract_image_metadata(local_path: str) -> Dict[str, Optional[Any]]:
    """
    Extract basic metadata from an image file using Pillow.
    Returns width/height/mode/format and keeps None on failure.
    """
    try:
        from PIL import Image

        with Image.open(local_path) as img:
            return {
                "width_px": int(img.size[0]) if img.size else None,
                "height_px": int(img.size[1]) if img.size else None,
                "color_mode": img.mode,
                "image_format": img.format,
            }
    except Exception as e:
        logger.warning("Could not read image metadata from %s: %s", local_path, e)
        return {
            "width_px": None,
            "height_px": None,
            "color_mode": None,
            "image_format": None,
        }


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # OCR setup (best-effort)
    ocr_endpoint = step_ctx.config.get("ocr", {}).get("endpoint")
    ocr_key = step_ctx.config.get("ocr", {}).get("api_key")
    ocr_extractor: Optional[FileExtractor] = None
    if ocr_endpoint:
        try:
            ocr_client = OCRClient(endpoint=ocr_endpoint, key=ocr_key)
            ocr_extractor = FileExtractor(ocr_client=ocr_client)
            logger.info("OCR enabled for visual ingestion.")
        except Exception as e:
            logger.warning(
                "OCR client initialization failed in s01_visual_ingestion: %s", e
            )
            ocr_extractor = None
    else:
        logger.warning("OCR endpoint not configured; image OCR will be skipped.")

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
            "ADLS Uploader could not be initialized in s01_visual_ingestion. "
            "Check managed identity RBAC and ADLS container permissions."
            + (f" Details: {detail}" if detail else "")
        )

    # Read latest discovery manifest from Silver.
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
        logger.error(f"Failed to list discovery manifests: {e}")
        manifest_paths = []

    if not manifest_paths:
        logger.warning(
            "No discovery manifest found in Silver (%s). Exiting.",
            indexing_source_list_prefix("s00_discovery"),
        )
        return

    latest_manifest = sorted(
        manifest_paths, key=lambda x: x.last_modified, reverse=True
    )[0]
    logger.info(f"Using discovery manifest: {latest_manifest.name}")

    try:
        file_client = silver_client.get_file_client(latest_manifest.name)
        manifest_data = json.loads(file_client.download_file().readall())
    except Exception as e:
        logger.error(f"Error reading manifest {latest_manifest.name}: {e}")
        return

    image_files = []
    for item in manifest_data:
        ext = (item.get("extension") or "").lower()
        if item.get("group") == "images" and ext in SUPPORTED_IMAGE_EXTENSIONS:
            image_files.append(item)

    logger.info(f"Found {len(image_files)} image files to process.")
    sl.set_input_count(len(image_files))

    results: List[Dict[str, Any]] = []
    now = datetime.now().isoformat()

    for item in image_files:
        file_path = item.get("file_path", "")
        staged_path = item.get("staged_path", "")
        filename = item.get("filename") or os.path.basename(file_path) or "unknown"
        ext = (item.get("extension") or "").lower()

        try:
            binary, read_from = _download_bytes_with_fallback(
                uploader=uploader,
                silver_client=silver_client,
                staged_path=staged_path,
                bronze_path=file_path,
            )
            image_size_bytes = len(binary) if binary else 0

            temp_fd, temp_local = tempfile.mkstemp(suffix=ext or "", prefix="img_")
            os.close(temp_fd)
            with open(temp_local, "wb") as f:
                f.write(binary)

            img_meta = _extract_image_metadata(temp_local)

            row = {
                "file_id": str(uuid.uuid5(uuid.NAMESPACE_URL, file_path or filename)),
                "file_name": filename,
                "source_path": file_path,
                "source_type": "image",
                "type_source": MIME_BY_EXTENSION.get(ext, "image/unknown"),
                "image_size_bytes": int(image_size_bytes),
                "width_px": img_meta.get("width_px"),
                "height_px": img_meta.get("height_px"),
                "color_mode": img_meta.get("color_mode"),
                "image_format": img_meta.get("image_format"),
                "metadata": {
                    "original_filename": filename,
                    "source_path": file_path,
                    "read_from": read_from,
                    "ingestion_date": now,
                    "source_step": "s01_visual_ingestion",
                },
            }

            mime_type = row["type_source"]
            ocr_text = ""
            if ocr_extractor and mime_type in OCR_SUPPORTED_MIME_TYPES:
                try:
                    ocr_text = (
                        ocr_extractor.extract(temp_local, mime_type) or ""
                    ).strip()
                except Exception as e:
                    logger.warning("OCR failed for image %s: %s", filename, e)
                    ocr_text = ""

            row["metadata"]["ocr_text"] = ocr_text
            row["metadata"]["ocr_characters"] = len(ocr_text)
            row["metadata"]["ocr_applied"] = bool(ocr_text)

            if os.path.exists(temp_local):
                os.remove(temp_local)

            results.append(row)
            sl.record_file(filename, "success", group="images")

        except Exception as e:
            logger.error(f"Error processing image {filename}: {e}")
            sl.record_error(filename, str(e))
            if "temp_local" in locals() and os.path.exists(temp_local):
                os.remove(temp_local)
            continue

    sl.set_output_count(len(results))

    if results:
        success = uploader.upload_json_data(
            json_data=results,
            remote_path="images_metadata.json",
            dir_path="ingestion/images",
            layer="silver",
            source_step="s01_visual_ingestion",
        )
        if success:
            logger.info(f"Uploaded metadata for {len(results)} images.")
        else:
            logger.error("Failed to upload image metadata.")
    else:
        logger.warning("No image metadata generated to upload.")


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
        step_name="s01_visual_ingestion",
    )
    process(step_ctx)
