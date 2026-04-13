"""
Sincroniza SharePoint → ADLS Bronze bajo el mismo prefijo que usa s00_discovery
(path_prefix de raw_documents en indexing_manifest.yaml), antes del discovery.
"""

import argparse
import asyncio
import concurrent.futures
import inspect
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

# -- Databricks path bootstrap --------------------------------------------------
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
from src.core.run_logger import StepLogger
from src.core.sharepoint.sharepoint_connection import SharePointToBronzeProcess

logger = get_logger(__name__)

STEP_NAME = "s00_sharepoint"


def _step_options(manifest: Dict[str, Any]) -> Dict[str, Any]:
    steps = manifest.get("workflow", {}).get("steps", {})
    step_def = steps.get(STEP_NAME, {})
    return step_def.get("options") or {}


def _discovery_raw_documents(manifest: Dict[str, Any]) -> Dict[str, Any]:
    steps = manifest.get("workflow", {}).get("steps", {})
    discovery = steps.get("s00_discovery", {})
    for inp in discovery.get("inputs", []):
        if inp.get("key") == "raw_documents":
            return inp
    return {}


def _bronze_folder_for_indexing(manifest: Dict[str, Any], sharepoint_cfg: Dict[str, Any]) -> str:
    """
    Prefijo bajo el contenedor bronze donde escribir blobs, alineado con s00_discovery.
    Prioridad: path_prefix del input raw_documents de s00_discovery; si vacío, sharepoint.bronze_folder.
    """
    raw = _discovery_raw_documents(manifest)
    pp = (raw.get("path_prefix") or "").strip().strip("/").replace("\\", "/")
    if pp:
        return pp
    bf = sharepoint_cfg.get("bronze_folder")
    if isinstance(bf, str) and bf.strip():
        return bf.strip().strip("/").replace("\\", "/")
    logger.warning(
        "Sin path_prefix en s00_discovery.raw_documents ni bronze_folder; "
        "SharePointToBronzeProcess usará el default derivado de relative_path."
    )
    return ""


def _parse_since_date(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return datetime.fromisoformat(value)
        except ValueError:
            logger.warning("since_date inválido (%s), se ignora", value)
            return None
    return None


def _coerce_max_folders(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


async def _run_sharepoint(
    opts: Dict[str, Any],
    sharepoint_config: Optional[Dict[str, Any]],
) -> Dict[str, int]:
    max_folders = _coerce_max_folders(opts.get("max_folders"))
    only_folder = opts.get("only_folder")
    if only_folder is not None and str(only_folder).strip() == "":
        only_folder = None
    elif only_folder is not None:
        only_folder = str(only_folder).strip()

    since_date = _parse_since_date(opts.get("since_date"))

    async with SharePointToBronzeProcess(sharepoint_config=sharepoint_config) as process:
        await process.run(
            max_folders=max_folders,
            only_folder=only_folder,
            since_date=since_date,
        )
        return process.last_sync_stats


def _run_coroutine_safely(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    def _runner():
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_runner).result()


def process(step_ctx: StepContext) -> None:
    with StepLogger(step_ctx) as sl:
        idx_cfg = step_ctx.config.get("indexing") or {}
        if idx_cfg.get("sharepoint_sync_enabled") is not True:
            logger.warning(
                "Sincronización SharePoint antes de indexing deshabilitada "
                "(config.indexing.sharepoint_sync_enabled != true); se omite el paso."
            )
            sl.set_step_metadata(
                {
                    "sync_ran": False,
                    "sync_disabled_reason": "indexing.sharepoint_sync_enabled",
                    "documents_synced": 0,
                    "skipped_unchanged": 0,
                    "skipped_since_date": 0,
                    "failed": 0,
                }
            )
            sl.set_input_count(0)
            sl.set_output_count(0)
            return

        sp_cfg = step_ctx.config.get("sharepoint") or {}
        if sp_cfg.get("enabled") is False:
            logger.warning(
                "SharePoint deshabilitado globalmente (config.sharepoint.enabled=false); "
                "se omite la sincronización para indexing."
            )
            sl.set_step_metadata(
                {
                    "sync_ran": False,
                    "sync_disabled_reason": "sharepoint.enabled",
                    "documents_synced": 0,
                    "skipped_unchanged": 0,
                    "skipped_since_date": 0,
                    "failed": 0,
                }
            )
            sl.set_input_count(0)
            sl.set_output_count(0)
            return

        bronze_target = _bronze_folder_for_indexing(step_ctx.manifest, sp_cfg)
        merged_sp = dict(sp_cfg)
        if bronze_target:
            merged_sp["bronze_folder"] = bronze_target
            logger.info(
                "Destino Bronze para indexing (contenedor bronze, prefijo): %s "
                "(alineado con s00_discovery raw_documents / path_prefix)",
                bronze_target,
            )

        opts = _step_options(step_ctx.manifest)
        logger.info("Opciones SharePoint desde manifest (%s): %s", STEP_NAME, opts)

        sl.set_input_count(1)
        stats = _run_coroutine_safely(_run_sharepoint(opts, merged_sp))
        sl.set_step_metadata(
            {
                "sync_ran": True,
                "sync_disabled_reason": None,
                "bronze_prefix": bronze_target or None,
                **stats,
            }
        )
        sl.set_output_count(int(stats.get("documents_synced", 0)))
        logger.info("Paso %s completado", STEP_NAME)


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
        step_name=STEP_NAME,
    )
    process(step_ctx)
