"""
s02_image_captioning.py
Generate textual captions for images discovered in s01_visual_ingestion.
"""

import argparse
import base64
import json
import os
import sys
import uuid
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

logger = get_logger(__name__)

try:
    from openai import AzureOpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AzureOpenAI = None  # type: ignore


def _guess_mime_type(source_path: str, fallback: str = "image/png") -> str:
    ext = (os.path.splitext(source_path or "")[1] or "").lower()
    mapping = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
        ".tiff": "image/tiff",
    }
    return mapping.get(ext, fallback)


def _get_llm_client(config: Dict[str, Any], llm_key: str) -> Optional[Any]:
    if not OPENAI_AVAILABLE:
        return None
    llm_config = config.get("llms", {}).get(llm_key, {})
    if not llm_config:
        return None
    endpoint = (llm_config.get("endpoint") or "").strip()
    api_key = (llm_config.get("api_key") or "").strip()
    api_version = (llm_config.get("api_version") or "2024-08-01-preview").strip()
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
        logger.warning("Could not create LLM client for image captioning: %s", e)
        return None


def _generate_caption(
    image_bytes: bytes,
    mime_type: str,
    file_name: str,
    client: Any,
    deployment_name: str,
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 180,
) -> str:
    if not image_bytes or not client or not deployment_name:
        return ""
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:{mime_type};base64,{image_b64}"

    user_text = prompt or "Describe la imagen de forma clara y objetiva."
    request_kwargs: Dict[str, Any] = {
        "model": deployment_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"{user_text}\n\nArchivo: {file_name}"},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    try:
        response = client.chat.completions.create(**request_kwargs)
    except Exception as e:
        err_txt = str(e).lower()
        if "max_tokens" in err_txt and "max_completion_tokens" in err_txt:
            retry_kwargs = dict(request_kwargs)
            retry_kwargs.pop("max_tokens", None)
            retry_kwargs["max_completion_tokens"] = max_tokens
            response = client.chat.completions.create(**retry_kwargs)
        else:
            raise

    choice = response.choices[0] if response.choices else None
    caption = (choice.message.content or "").strip() if choice else ""
    return caption


def _extract_ocr_text_from_bytes(
    image_bytes: bytes, ocr_client: Optional[OCRClient]
) -> str:
    if not image_bytes or not ocr_client:
        return ""
    try:
        return (ocr_client.extract_text(image_bytes) or "").strip()
    except Exception as e:
        logger.warning("OCR extraction failed during image captioning: %s", e)
        return ""


def _load_latest_images_metadata(silver_client) -> List[Dict[str, Any]]:
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s01_visual_ingestion"),
                recursive=True,
            )
        )
        file_paths = [
            p for p in paths if not p.is_directory and "images_metadata.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list images metadata paths: {e}")
        return []

    if not file_paths:
        return []

    latest_file = sorted(file_paths, key=lambda x: x.last_modified, reverse=True)[0]
    logger.info(f"Using images metadata input file: {latest_file.name}")

    try:
        file_client = silver_client.get_file_client(latest_file.name)
        download = file_client.download_file()
        data = json.loads(download.readall())
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Could not read input {latest_file.name}: {e}")
        return []


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    caption_cfg = step_ctx.config.get("processing", {}).get("image_captioning", {})
    captioning_enabled = bool(caption_cfg.get("enabled", True))
    llm_key = caption_cfg.get("llm_key", "gpt4_reasoner")
    prompt = caption_cfg.get(
        "prompt",
        "Describe la imagen de forma clara y objetiva, en español, en máximo 3 frases.",
    )

    llm_client = None
    llm_deployment = ""
    llm_temperature = 0.0
    llm_max_tokens = 180
    if captioning_enabled:
        llm_client = _get_llm_client(step_ctx.config, llm_key)
        if llm_client:
            llm_cfg = step_ctx.config.get("llms", {}).get(llm_key, {})
            llm_deployment = (llm_cfg.get("deployment_name") or "").strip()
            llm_temperature = float(llm_cfg.get("temperature", 0.0))
            llm_max_tokens = int(llm_cfg.get("max_tokens", 2000))
            if llm_deployment:
                logger.info(
                    "Image captioning enabled: llm_key=%s, deployment=%s",
                    llm_key,
                    llm_deployment,
                )
            else:
                logger.warning(
                    "Image captioning enabled but llms.%s.deployment_name missing; captions will use fallback text",
                    llm_key,
                )
        else:
            logger.warning(
                "Image captioning enabled but LLM client unavailable; captions will use fallback text"
            )
    else:
        logger.info(
            "Image captioning disabled by config (processing.image_captioning.enabled=false)"
        )

    # OCR setup (best-effort)
    ocr_endpoint = step_ctx.config.get("ocr", {}).get("endpoint")
    ocr_key = step_ctx.config.get("ocr", {}).get("api_key")
    ocr_client = None
    if ocr_endpoint:
        try:
            ocr_client = OCRClient(endpoint=ocr_endpoint, key=ocr_key)
            logger.info("OCR enabled for image captioning (OCR-first strategy).")
        except Exception as e:
            logger.warning(
                "OCR client initialization failed in s02_image_captioning: %s", e
            )
    else:
        logger.warning("OCR endpoint not configured; OCR-first strategy unavailable.")

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
            "ADLS Uploader could not be initialized in s02_image_captioning. "
            "Check managed identity RBAC and ADLS container permissions."
            + (f" Details: {detail}" if detail else "")
        )

    silver_client = uploader.get_layer_client("silver")
    images_meta = _load_latest_images_metadata(silver_client)
    sl.set_input_count(len(images_meta))

    if not images_meta:
        logger.warning(
            "No image metadata found for captioning. Uploading empty output."
        )
        uploader.upload_json_data(
            json_data=[],
            remote_path="image_captions.json",
            dir_path="processing/image_captions",
            layer="silver",
            source_step="s02_image_captioning",
        )
        sl.set_output_count(0)
        return

    captions: List[Dict[str, Any]] = []
    failed = 0

    for row in images_meta:
        file_id = row.get("file_id") or ""
        file_name = row.get("file_name") or "unknown"
        source_path = row.get("source_path") or ""
        mime_type = row.get("type_source") or _guess_mime_type(source_path)
        if not source_path:
            failed += 1
            sl.record_error(file_name, "Missing source_path in images metadata")
            continue

        try:
            file_client = uploader.container_client.get_file_client(source_path)
            image_bytes = file_client.download_file().readall()

            # OCR-first: if we can extract useful text from image, use it as content.
            caption = ""
            ocr_text = (
                (row.get("metadata", {}) or {}).get("ocr_text")
                if isinstance(row.get("metadata"), dict)
                else ""
            ) or ""
            ocr_text = (ocr_text or "").strip()
            if not ocr_text:
                ocr_text = _extract_ocr_text_from_bytes(
                    image_bytes=image_bytes, ocr_client=ocr_client
                )

            if ocr_text:
                caption = ocr_text
            elif captioning_enabled and llm_client and llm_deployment:
                caption = _generate_caption(
                    image_bytes=image_bytes,
                    mime_type=mime_type,
                    file_name=file_name,
                    client=llm_client,
                    deployment_name=llm_deployment,
                    prompt=prompt,
                    temperature=llm_temperature,
                    max_tokens=min(llm_max_tokens, 180),
                )

            if not caption:
                # Keep pipeline flowing even if captioning is disabled/unavailable/fails.
                caption = f"Imagen {file_name} ubicada en {source_path}."

            captions.append(
                {
                    "file_id": file_id
                    or str(uuid.uuid5(uuid.NAMESPACE_URL, source_path)),
                    "file_name": file_name,
                    "source_path": source_path,
                    "source_type": "text",
                    "content": caption,
                    "metadata": {
                        "origin": "image_ocr" if ocr_text else "image_captioning",
                        "mime_type": mime_type,
                        "source_step": "s02_image_captioning",
                        "ocr_applied": bool(ocr_text),
                    },
                    "run_id": step_ctx.run_id,
                }
            )
            sl.record_file(file_name, "success", group="images")

        except Exception as e:
            failed += 1
            logger.error(f"Error generating caption for image {file_name}: {e}")
            sl.record_error(file_name, str(e))

    sl.set_output_count(len(captions))
    logger.info(
        "Image captioning completed: success=%s, failed=%s", len(captions), failed
    )

    uploader.upload_json_data(
        json_data=captions,
        remote_path="image_captions.json",
        dir_path="processing/image_captions",
        layer="silver",
        source_step="s02_image_captioning",
    )
    logger.info(f"Uploaded {len(captions)} image captions to Silver.")


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
        step_name="s02_image_captioning",
    )
    process(step_ctx)
