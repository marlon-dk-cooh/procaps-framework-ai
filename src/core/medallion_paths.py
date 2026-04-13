"""
Convenciones de rutas en ADLS (filesystem silver) para el pipeline de indexing.

- Artefactos intermedios: steps_indexing/source=<step>/year=.../...
- Logs de StepLogger por run: logs_indexing/<run_id>/ (solo steps de indexing).
"""

from __future__ import annotations

import re
from typing import Optional

INDEXING_STEPS_ROOT = "steps_indexing"
INDEXING_LOGS_ROOT = "logs_indexing"

# Plantilla new_workflow: no usar prefijo de indexing
_EXCLUDED_INDEXING_STEPS = frozenset({"s00_ingestion", "s01_preprocess"})


def is_indexing_medallion_source(source_step: Optional[str]) -> bool:
    """True si los artefactos silver van bajo steps_indexing/ (upload_json_data, staging)."""
    if not source_step or source_step in _EXCLUDED_INDEXING_STEPS:
        return False
    return bool(re.match(r"^s\d{2}_", source_step))


def step_logger_silver_dir(run_id: str, step_name: str) -> str:
    """Directorio bajo filesystem silver para el JSON del StepLogger (sin nombre de archivo)."""
    if is_indexing_medallion_source(step_name):
        return f"{INDEXING_LOGS_ROOT}/{run_id}"
    return f"logs/{run_id}"


def indexing_source_list_prefix(source_step: str) -> str:
    """Prefijo para get_paths en silver (termina en /)."""
    return f"{INDEXING_STEPS_ROOT}/source={source_step}/"
