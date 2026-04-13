"""
Pipeline Context
----------------
Provides a context object for each pipeline step.
It contains configuration, manifest, and runtime identifiers.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from .logging_config import get_logger
from .colombian_time import current_colombian_time

logger = get_logger(__name__)


@dataclass
class StepContext:
    """
    Context object passed to each pipeline step.
    Contains configuration, manifest, and runtime identifiers.
    """

    config: Dict[str, Any]
    manifest: Dict[str, Any]
    run_id: str
    task_run_id: str
    step_name: str
    execution_date: str = field(
        default_factory=lambda: current_colombian_time().isoformat()
    )

    def get_input(self, input_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve input definition from manifest for this step."""
        step_def = (
            self.manifest.get("workflow", {}).get("steps", {}).get(self.step_name, {})
        )
        inputs = step_def.get("inputs", [])
        for inp in inputs:
            if inp.get("key") == input_key:
                return inp
        return None

    def get_output(self, output_key: str) -> Optional[Dict[str, Any]]:
        """Retrieve output definition from manifest for this step."""
        step_def = (
            self.manifest.get("workflow", {}).get("steps", {}).get(self.step_name, {})
        )
        outputs = step_def.get("outputs", [])
        for out in outputs:
            if out.get("key") == output_key:
                return out
        return None
