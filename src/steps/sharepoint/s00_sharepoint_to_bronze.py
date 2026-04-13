"""
Copia archivos desde SharePoint hacia la capa Bronze en ADLS usando SharePointToBronzeProcess.
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

STEP_NAME = "s00_sharepoint_to_bronze"


def _step_options(manifest: Dict[str, Any]) -> Dict[str, Any]:
    steps = manifest.get("workflow", {}).get("steps", {})
    step_def = steps.get(STEP_NAME, {})
    return step_def.get("options") or {}


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
) -> None:
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


def _run_coroutine_safely(coro) -> None:
    """
    Ejecuta una corrutina cuando el proceso puede tener ya un event loop activo
    (p. ej. kernel de notebook en Databricks). asyncio.run() falla en ese caso;
    se delega a un hilo con un loop nuevo.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return

    def _runner() -> None:
        asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(_runner).result()


def process(step_ctx: StepContext) -> None:
    with StepLogger(step_ctx) as sl:
        sp_cfg = step_ctx.config.get("sharepoint") or {}
        if sp_cfg.get("enabled") is False:
            logger.warning(
                "Workflow SharePoint deshabilitado (config.sharepoint.enabled=false); se omite la sincronización."
            )
            sl.set_input_count(0)
            sl.set_output_count(0)
            return

        opts = _step_options(step_ctx.manifest)
        logger.info("Opciones SharePoint desde manifest: %s", opts)

        raw_input = step_ctx.get_input("sharepoint_source") or {}
        logger.info("Entrada manifest sharepoint_source: %s", raw_input)

        sl.set_input_count(1)
        _run_coroutine_safely(_run_sharepoint(opts, step_ctx.config.get("sharepoint")))
        sl.set_output_count(1)
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
