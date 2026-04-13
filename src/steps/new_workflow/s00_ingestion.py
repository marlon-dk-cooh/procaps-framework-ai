"""
Plantilla: primer task de un workflow nuevo.
Duplica la carpeta `new_workflow` con el nombre del nuevo caso y renombra este módulo si cambia el task_key en el job YAML.
"""

import argparse
import os
import sys
from typing import Any, Dict

import inspect

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

logger = get_logger(__name__)

STEP_NAME = "s00_ingestion"


def process(step_ctx: StepContext) -> None:
    with StepLogger(step_ctx) as sl:
        _process_internal(step_ctx, sl)


def _process_internal(step_ctx: StepContext, sl) -> None:
    logger.info("Iniciando step %s (plantilla)", step_ctx.step_name)

    raw_input: Dict[str, Any] = step_ctx.get_input("raw_input") or {}
    logger.info(
        "Definicion de entrada 'raw_input' desde manifest: %s",
        raw_input,
    )

    # Implementar: leer origen (ADLS, volumen UC, tablas, etc.) segun raw_input
    # Implementar: escribir salida coherente con manifest (key: ingested_data)
    sl.set_input_count(0)
    sl.set_output_count(0)
    logger.warning(
        "%s: logica pendiente — revisa config/new_manifest.yaml y este script.",
        STEP_NAME, 
    )


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
