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

logger = get_logger(__name__)


def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger

    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl):
    logger.info(f"Starting Step: {step_ctx.step_name}")

    # Config
    chunk_config = step_ctx.config.get("processing", {}).get("chunking", {})
    # Nombre de parámetro en config para seleccionar el tipo de chunking: type_chunking
    type_chunking = chunk_config.get("type_chunking", "recursive")
    strategy_overrides = chunk_config.get("strategy_overrides", {})

    chunk_size = chunk_config.get("chunk_size", 1000)
    chunk_overlap = chunk_config.get("chunk_overlap", 100)
    chunk_size_words = chunk_config.get("chunk_size_words")
    chunk_overlap_words = chunk_config.get("chunk_overlap_words", 0)
    min_words = chunk_config.get("min_words")
    strip_whitespace = chunk_config.get("strip_whitespace", True)
    normalize_spaces = chunk_config.get("normalize_spaces", True)

    size = chunk_config.get("size")
    overlap = chunk_config.get("overlap")
    separator = chunk_config.get("separator")
    is_separator_regex = chunk_config.get("is_separator_regex", False)

    separators = chunk_config.get("separators")
    keep_separator = chunk_config.get("keep_separator", True)
    recursive_is_separator_regex = chunk_config.get(
        "recursive_is_separator_regex", False
    )

    markdown_headers = chunk_config.get("markdown_headers")
    extra_params = chunk_config.get("extra_params", {})
    semantic_threshold_type = chunk_config.get("semantic_threshold_type", "percentile")
    semantic_breakpoint_threshold_amount = chunk_config.get(
        "semantic_breakpoint_threshold_amount", 95.0
    )
    semantic_embedding_model = chunk_config.get("semantic_embedding_model")

    logger.info(
        "Chunking config loaded | type_chunking=%s | strategy_overrides=%s",
        type_chunking,
        strategy_overrides,
    )

    def _strategy_params_for_log(strategy: str) -> dict:
        if strategy == "fixed_word":
            return {
                "chunk_size_words": chunk_size_words,
                "chunk_overlap_words": chunk_overlap_words,
                "min_words": min_words,
                "strip_whitespace": strip_whitespace,
                "normalize_spaces": normalize_spaces,
                "fallback_chunk_size": chunk_size,
                "fallback_chunk_overlap": chunk_overlap,
            }
        if strategy == "character":
            return {
                "size": size if size is not None else f"fallback:{chunk_size}",
                "overlap": overlap
                if overlap is not None
                else f"fallback:{chunk_overlap}",
                "separator": separator if separator is not None else "\\n\\n",
                "is_separator_regex": is_separator_regex,
            }
        if strategy == "recursive":
            return {
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "separators": separators,
                "keep_separator": keep_separator,
                "recursive_is_separator_regex": recursive_is_separator_regex,
            }
        if strategy == "markdown":
            return {
                "chunk_size": chunk_size,
                "chunk_overlap": chunk_overlap,
                "markdown_headers": markdown_headers,
            }
        if strategy == "semantic":
            return {
                "semantic_threshold_type": semantic_threshold_type,
                "semantic_breakpoint_threshold_amount": semantic_breakpoint_threshold_amount,
                "semantic_embedding_model": semantic_embedding_model
                or "all-MiniLM-L6-v2",
            }
        return {"chunk_size": chunk_size, "chunk_overlap": chunk_overlap}

    from src.core.chunking.router import get_chunker
    from src.core.chunking.base import ChunkingConfig
    from src.core.ir_models import DocumentIR, Element
    from dataclasses import asdict

    uploader = get_uploader()
    if not uploader.initialized:
        pass

    if not uploader.initialized:
        try:
            from src.core.secret_helper import get_secret, SecretNames

            acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
            uploader.initialize(account_name=acct, container_name="bronze")
        except Exception:
            pass

    # Read inputs from Silver:
    # - textual IR from s01_textual
    # - structured rows from s01_structured_ingestion (optional)
    silver_client = uploader.get_layer_client("silver")
    try:
        paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s01_textual"), recursive=True
            )
        )
        file_paths = [
            p for p in paths if not p.is_directory and "ir_data.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list paths: {e}")
        file_paths = []

    data = []
    if file_paths:
        latest_file = sorted(file_paths, key=lambda x: x.last_modified, reverse=True)[0]
        logger.info(f"Using textual input file: {latest_file.name}")
        try:
            file_client = silver_client.get_file_client(latest_file.name)
            download = file_client.download_file()
            data = json.loads(download.readall())
        except Exception as e:
            logger.error(f"Could not read input file {latest_file.name}: {e}")
            raise e
    else:
        logger.warning("No textual ir_data.json found from s01_textual.")

    # Optional tabular input (flattened rows with `content` + metadata)
    try:
        t_paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s01_tabular_ingestion"),
                recursive=True,
            )
        )
        t_file_paths = [
            p for p in t_paths if not p.is_directory and "tabular_data.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list tabular paths: {e}")
        t_file_paths = []

    tabular_docs = []
    if t_file_paths:
        t_latest = sorted(t_file_paths, key=lambda x: x.last_modified, reverse=True)[0]
        logger.info(f"Using tabular input file: {t_latest.name}")
        try:
            t_file_client = silver_client.get_file_client(t_latest.name)
            t_download = t_file_client.download_file()
            t_data = json.loads(t_download.readall())
            if isinstance(t_data, list):
                for row in t_data:
                    if not isinstance(row, dict):
                        continue
                    content = (row.get("content") or "").strip()
                    if not content:
                        continue
                    tabular_docs.append(
                        {
                            "doc_id": row.get("file_id")
                            or row.get("file_name")
                            or "tabular-unknown",
                            "source_path": row.get("source_path") or "",
                            "plain_text": content,
                            "markdown_text": None,
                            "structured_elements": [],
                            "metadata": {
                                "mime_type": f"application/{row.get('file_type', 'csv')}",
                                "normalization_method": "tabular_ingestion",
                                "original_filename": row.get("file_name") or "unknown",
                                "source_path": row.get("source_path") or "",
                                "record_metadata": row.get("metadata") or {},
                            },
                        }
                    )
        except Exception as e:
            logger.error(f"Could not read tabular input file {t_latest.name}: {e}")

    if tabular_docs:
        data.extend(tabular_docs)
        logger.info(f"Added tabular docs for chunking: {len(tabular_docs)}")

    # Optional structured input (already flattened rows with `content` + metadata)
    try:
        s_paths = list(
            silver_client.get_paths(
                path=indexing_source_list_prefix("s01_structured_ingestion"),
                recursive=True,
            )
        )
        s_file_paths = [
            p
            for p in s_paths
            if not p.is_directory and "structured_data.json" in p.name
        ]
    except Exception as e:
        logger.error(f"Failed to list structured paths: {e}")
        s_file_paths = []

    structured_docs = []
    if s_file_paths:
        s_latest = sorted(s_file_paths, key=lambda x: x.last_modified, reverse=True)[0]
        logger.info(f"Using structured input file: {s_latest.name}")
        try:
            s_file_client = silver_client.get_file_client(s_latest.name)
            s_download = s_file_client.download_file()
            s_data = json.loads(s_download.readall())
            if isinstance(s_data, list):
                for row in s_data:
                    if not isinstance(row, dict):
                        continue
                    content = (row.get("content") or "").strip()
                    if not content:
                        continue
                    structured_docs.append(
                        {
                            "doc_id": row.get("file_id")
                            or row.get("file_name")
                            or "structured-unknown",
                            "source_path": row.get("source_path") or "",
                            "plain_text": content,
                            "markdown_text": None,
                            "structured_elements": [],
                            "metadata": {
                                "mime_type": f"application/{row.get('file_type', 'json')}",
                                "normalization_method": "structured_ingestion",
                                "original_filename": row.get("file_name") or "unknown",
                                "source_path": row.get("source_path") or "",
                                "record_metadata": row.get("metadata") or {},
                            },
                        }
                    )
        except Exception as e:
            logger.error(f"Could not read structured input file {s_latest.name}: {e}")

    if structured_docs:
        data.extend(structured_docs)
        logger.info(f"Added structured docs for chunking: {len(structured_docs)}")

    if not data:
        logger.warning("No input data found for chunking. Exiting.")
        return

    chunks_results = []
    sl.set_input_count(len(data))

    for doc_dict in data:
        # Rehydrate DocumentIR
        # Need to handle structured_elements list of dicts -> List[Element]
        elements = []
        if doc_dict.get("structured_elements"):
            for el_dict in doc_dict["structured_elements"]:
                elements.append(Element(**el_dict))

        doc_dict["structured_elements"] = elements
        ir = DocumentIR(**doc_dict)

        # Determine strategy
        # 1. File type override?
        # 2. Config override?
        # 3. Default

        strategy_name = type_chunking
        mime_type = ir.metadata.get("mime_type", "")

        # Simple wildcard matching for overrides (e.g. "*.md" or "text/markdown")
        # TODO: strict glob matching if needed
        if mime_type == "text/markdown":
            strategy_name = strategy_overrides.get("*.md", strategy_name)  # simplified
            if "markdown" in strategy_overrides.values():  # or check specific key
                pass

        # Creating Config object
        config_obj = ChunkingConfig(
            strategy=strategy_name,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunk_size_words=chunk_size_words,
            chunk_overlap_words=chunk_overlap_words,
            min_words=min_words,
            strip_whitespace=strip_whitespace,
            normalize_spaces=normalize_spaces,
            size=size,
            overlap=overlap,
            separator=separator,
            is_separator_regex=is_separator_regex,
            separators=separators,
            keep_separator=keep_separator,
            recursive_is_separator_regex=recursive_is_separator_regex,
            markdown_headers=markdown_headers,
            semantic_threshold_type=semantic_threshold_type,
            semantic_breakpoint_threshold_amount=semantic_breakpoint_threshold_amount,
            semantic_embedding_model=semantic_embedding_model,
            extra_params=extra_params,
        )

        original_filename = ir.metadata.get("original_filename", ir.doc_id or "unknown")
        logger.info(
            "Chunking document | doc_id=%s | file=%s | mime_type=%s | strategy=%s | params=%s",
            ir.doc_id,
            original_filename,
            mime_type or "unknown",
            strategy_name,
            _strategy_params_for_log(strategy_name),
        )

        try:
            strategy = get_chunker(config_obj)
            chunks = strategy.chunk(ir, config_obj)
            logger.info(
                "Chunking result | doc_id=%s | strategy=%s | chunks_generated=%s",
                ir.doc_id,
                strategy_name,
                len(chunks),
            )

            for chunk in chunks:
                chunks_results.append(asdict(chunk))

            sl.record_file(original_filename, "success", group=strategy_name)

        except Exception as e:
            logger.error(f"Chunking failed for {ir.doc_id} using {strategy_name}: {e}")
            sl.record_error(original_filename, str(e))
            continue

    sl.set_output_count(len(chunks_results))

    # Upload Outputs
    if chunks_results:
        success = uploader.upload_json_data(
            json_data=chunks_results,
            remote_path="chunks_data.json",
            dir_path="processing/chunks",
            layer="silver",
            source_step="s02_chunking",
        )
        if success:
            logger.info(f"Uploaded {len(chunks_results)} chunks to Silver")
        else:
            logger.error("Failed to upload chunks")
    else:
        logger.warning("No chunks generated")


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
        config, manifest, args.run_id, args.task_run_id, step_name="s02_chunking"
    )

    process(step_ctx)
