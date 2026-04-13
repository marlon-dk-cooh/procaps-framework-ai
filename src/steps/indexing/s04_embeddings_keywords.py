import argparse
import os
import sys
import json
import re
from collections import Counter
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

from src.core.settings import load_config, load_manifest
from src.core.pipeline_context import StepContext
from src.core.logging_config import get_logger
from src.core.adls_uploader import get_uploader
from src.core.medallion_paths import indexing_source_list_prefix
from src.core.embeddings import EmbeddingsGenerator

logger = get_logger(__name__)

try:
    from openai import AzureOpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AzureOpenAI = None  # type: ignore

# Maximum characters of chunk content to send to LLM for keyword extraction (avoid token limits)
_KEYWORDS_CONTENT_MAX_CHARS = 3000

KEYWORDS_SYSTEM_PROMPT = (
    "You are a keyword extractor. Given a text fragment, output only 3 to 5 short keywords or key phrases "
    "that best describe its content. Output nothing else: only the keywords separated by commas, in the same language as the text."
)

_STOPWORDS_ES = {
    "de",
    "la",
    "el",
    "los",
    "las",
    "un",
    "una",
    "unos",
    "unas",
    "y",
    "o",
    "u",
    "e",
    "a",
    "ante",
    "bajo",
    "con",
    "contra",
    "desde",
    "durante",
    "en",
    "entre",
    "hacia",
    "hasta",
    "para",
    "por",
    "segun",
    "sin",
    "sobre",
    "tras",
    "que",
    "como",
    "cuando",
    "donde",
    "quien",
    "cual",
    "cuales",
    "es",
    "son",
    "fue",
    "fueron",
    "ser",
    "se",
    "su",
    "sus",
    "al",
    "del",
    "lo",
    "le",
    "les",
    "ya",
    "mas",
    "muy",
    "pero",
    "si",
    "no",
    "ni",
    "este",
    "esta",
    "estos",
    "estas",
    "ese",
    "esa",
    "esos",
    "esas",
}


def _fallback_keywords_from_text(content: str, max_keywords: int = 5) -> List[str]:
    """
    Lightweight fallback keywords extractor when LLM output is unavailable.
    Uses word frequency over normalized tokens and returns top terms.
    """
    if not content:
        return []
    tokens = re.findall(r"[A-Za-zÁÉÍÓÚáéíóúÑñÜü]{4,}", content.lower())
    if not tokens:
        return []
    filtered = [t for t in tokens if t not in _STOPWORDS_ES]
    if not filtered:
        return []
    counts = Counter(filtered)
    top = [w for w, _ in counts.most_common(max_keywords)]
    return top[:max_keywords]


def _parse_keywords_response(raw: str, max_keywords: int = 5) -> List[str]:
    """
    Parse model output into keywords supporting JSON list, comma/newline/semicolon formats.
    """
    if not raw:
        return []
    text = raw.strip()

    # JSON array support: ["k1", "k2"]
    if text.startswith("[") and text.endswith("]"):
        try:
            obj = json.loads(text)
            if isinstance(obj, list):
                kws = [str(x).strip() for x in obj if str(x).strip()]
                return kws[:max_keywords]
        except Exception:
            pass

    # Common delimiters: comma, newline, semicolon
    parts = re.split(r"[,;\n]+", text)
    cleaned = []
    for p in parts:
        item = p.strip()
        item = re.sub(r"^[-*•\d\.\)\(]+\s*", "", item)
        if item:
            cleaned.append(item)

    # Deduplicate preserving order
    unique = []
    seen = set()
    for item in cleaned:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    return unique[:max_keywords]


def _load_latest_json_from_source(
    silver_client, source_step: str, target_file: str
) -> List[Dict[str, Any]]:
    """
    Read latest JSON artifact for a source step in Silver.
    Returns [] when not found or on parsing errors.
    """
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix(source_step), recursive=True
            )
        )
        file_paths = [p for p in paths if not p.is_directory and target_file in p.name]
    except Exception as e:
        logger.error("Failed to list paths for source=%s: %s", source_step, e)
        return []

    if not file_paths:
        return []

    latest_file = sorted(file_paths, key=lambda x: x.last_modified, reverse=True)[0]
    logger.info("Using input file for %s: %s", source_step, latest_file.name)
    try:
        file_client = silver_client.get_file_client(latest_file.name)
        download = file_client.download_file()
        data = json.loads(download.readall())
        if isinstance(data, list):
            return data
        logger.warning("Input file %s is not a list JSON. Ignoring.", latest_file.name)
        return []
    except Exception as e:
        logger.error("Could not read input %s: %s", latest_file.name, e)
        return []


def _to_embedding_item_from_caption(row: Dict[str, Any], idx: int) -> Dict[str, Any]:
    """
    Normalize image caption row into the same shape expected from sanitized chunks.
    """
    file_id = row.get("file_id") or f"image-caption-{idx}"
    file_name = row.get("file_name") or file_id
    source_path = row.get("source_path") or file_name
    content = row.get("content") or ""
    meta_in = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
    mime_type = meta_in.get("mime_type") or "image/unknown"

    return {
        "chunk_id": f"{file_id}-caption",
        "content": content,
        "metadata": {
            "source_doc_id": file_id,
            "chunk_ordinal": 0,
            "strategy": "image_captioning",
            "original_metadata": {
                "original_filename": file_name,
                "source_path": source_path,
                "mime_type": mime_type,
            },
            "source_step": "s02_image_captioning",
            "source_type": "image",
            "is_image_caption": True,
        },
    }


def _get_llm_client(config: Dict[str, Any], llm_key: str) -> Optional[Any]:
    """Build Azure OpenAI client from config.llms[llm_key]. Returns None if not configured or openai missing."""
    if not OPENAI_AVAILABLE:
        return None
    llm_config = config.get("llms", {}).get(llm_key, {})
    if not llm_config:
        return None
    endpoint = llm_config.get("endpoint") or ""
    api_key = llm_config.get("api_key") or ""
    api_version = llm_config.get("api_version") or "2024-08-01-preview"
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
        logger.warning("Could not create LLM client for keywords: %s", e)
        return None


def _generate_keywords_for_chunk(
    content: str,
    client: Any,
    deployment_name: str,
    max_keywords: int = 5,
    temperature: float = 0.0,
    max_tokens: int = 150,
) -> List[str]:
    """
    Ask the LLM to extract keywords from chunk content. Returns a list of keyword strings.
    On failure returns empty list.
    """
    if not content or not client or not deployment_name:
        return []
    text = content.strip()
    if len(text) > _KEYWORDS_CONTENT_MAX_CHARS:
        text = text[:_KEYWORDS_CONTENT_MAX_CHARS].rstrip() + "…"
    user_prompt = (
        f"Extract up to {max_keywords} keywords from the following text.\n\n{text}"
    )
    request_kwargs: Dict[str, Any] = {
        "model": deployment_name,
        "messages": [
            {"role": "system", "content": KEYWORDS_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    fallback = _fallback_keywords_from_text(content=content, max_keywords=max_keywords)

    try:
        response = client.chat.completions.create(**request_kwargs)
    except Exception as e:
        err_txt = str(e).lower()
        if "max_tokens" in err_txt and "max_completion_tokens" in err_txt:
            try:
                retry_kwargs = {**request_kwargs}
                retry_kwargs.pop("max_tokens", None)
                retry_kwargs["max_completion_tokens"] = max_tokens
                response = client.chat.completions.create(**retry_kwargs)
            except Exception as retry_e:
                logger.warning(
                    "Keyword extraction failed for chunk (using fallback): %s", retry_e
                )
                return fallback
        else:
            logger.warning(
                "Keyword extraction failed for chunk (using fallback): %s", e
            )
            return fallback
    choice = response.choices[0] if response.choices else None
    raw = (choice.message.content or "").strip() if choice else ""
    parsed = _parse_keywords_response(raw=raw, max_keywords=max_keywords)
    if parsed:
        return parsed
    return fallback


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Config
    emb_config = step_ctx.config.get("embeddings", {}).get("embeddings_large", {})
    endpoint = emb_config.get("endpoint")
    key = emb_config.get("api_key")
    version = emb_config.get("api_version")
    deployment = emb_config.get("deployment_name")
    batch_size = emb_config.get("batch_size", 20)

    keywords_config = step_ctx.config.get("embeddings", {}).get("keywords", {})
    keywords_enabled = keywords_config.get("enabled", False)
    llm_key = keywords_config.get("llm_key", "gpt4_reasoner")
    max_keywords = int(keywords_config.get("max_keywords", 5))
    llm_client = None
    llm_deployment = None
    llm_temperature = 0.0
    llm_max_tokens = 150
    if keywords_enabled:
        llm_client = _get_llm_client(step_ctx.config, llm_key)
        if llm_client:
            llm_cfg = step_ctx.config.get("llms", {}).get(llm_key, {})
            llm_deployment = (llm_cfg.get("deployment_name") or "").strip()
            llm_temperature = float(llm_cfg.get("temperature", 0.0))
            llm_max_tokens = int(llm_cfg.get("max_tokens", 2000))
            if not llm_deployment:
                logger.warning(
                    "keywords enabled but llms.%s.deployment_name missing or empty; skipping keyword generation",
                    llm_key,
                )
                llm_client = None
            else:
                logger.info(
                    "Keyword generation enabled: llm_key=%s, deployment=%s, max_keywords=%s",
                    llm_key,
                    llm_deployment,
                    max_keywords,
                )
        else:
            logger.warning(
                "keywords enabled but LLM client could not be created; keywords will be empty"
            )
    else:
        logger.info(
            "Keyword generation disabled (embeddings.keywords.enabled=false); keywords will be empty"
        )

    generator = EmbeddingsGenerator(
        endpoint=endpoint, deployment=deployment, api_version=version, key=key
    )

    uploader = get_uploader()
    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception:
            pass

    # Read inputs from Silver:
    # - sanitized chunks (s03)
    # - image captions (s02)
    silver_client = uploader.get_layer_client("silver")
    sanitized_data = _load_latest_json_from_source(
        silver_client=silver_client,
        source_step="s03_sanitization",
        target_file="sanitized_chunks.json",
    )
    image_caption_rows = _load_latest_json_from_source(
        silver_client=silver_client,
        source_step="s02_image_captioning",
        target_file="image_captions.json",
    )
    caption_items = [
        _to_embedding_item_from_caption(row, idx)
        for idx, row in enumerate(image_caption_rows)
        if isinstance(row, dict) and (row.get("content") or "").strip()
    ]

    data = sanitized_data + caption_items
    if not data:
        logger.warning(
            "No input data found for embeddings (sanitized_chunks + image_captions). Exiting."
        )
        return

    logger.info(
        "Embedding input rows: sanitized=%s, image_captions=%s, total=%s",
        len(sanitized_data),
        len(caption_items),
        len(data),
    )

    embeddings_results = []
    sl.set_input_count(len(data))

    # Batch processing
    batch_buffer = []
    batch_indices = []
    unique_sources = set()

    for idx, item in enumerate(data):
        meta = item.get("metadata", {})
        orig_filename = (
            meta.get("original_metadata", {}).get("original_filename")
            or meta.get("source_doc_id")
            or "unknown"
        )
        if orig_filename != "unknown":
            unique_sources.add(orig_filename)

        batch_buffer.append(item["content"])
        batch_indices.append(idx)

        if len(batch_buffer) >= batch_size or idx == len(data) - 1:
            try:
                vectors = generator.generate(batch_buffer)

                for i, vector in enumerate(vectors):
                    original_idx = batch_indices[i]
                    original_item = data[original_idx]

                    new_item = original_item.copy()
                    new_item["vector"] = vector
                    new_item["metadata"]["embedded"] = True
                    new_item["metadata"]["source_step"] = "s04_embeddings_keywords"

                    if keywords_enabled and llm_client and llm_deployment:
                        kw = _generate_keywords_for_chunk(
                            original_item.get("content") or "",
                            llm_client,
                            llm_deployment,
                            max_keywords=max_keywords,
                            temperature=llm_temperature,
                            max_tokens=min(llm_max_tokens, 150),
                        )
                        new_item["metadata"]["keywords"] = kw
                    else:
                        new_item["metadata"]["keywords"] = (
                            new_item["metadata"].get("keywords") or []
                        )

                    embeddings_results.append(new_item)

            except Exception as e:
                logger.error(f"Batch failed: {e}")

            batch_buffer = []
            batch_indices = []

    for source in unique_sources:
        sl.record_file(source, "success")

    sl.set_output_count(len(embeddings_results))

    # Upload
    if embeddings_results:
        uploader.upload_json_data(
            json_data=embeddings_results,
            remote_path="embeddings_data.json",
            dir_path="processing/embeddings",
            layer="silver",
            source_step="s04_embeddings_keywords",
        )
        logger.info(f"Uploaded {len(embeddings_results)} embeddings")


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
        step_name="s04_embeddings_keywords",
    )

    process(step_ctx)
