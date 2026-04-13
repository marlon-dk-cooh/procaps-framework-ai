"""
Run Logger
----------
Provides a context-manager pattern for logging pipeline step execution.
It captures input/output counts, file processing status, errors, and costs.
"""

import json
import traceback
from typing import Any, Dict, Optional

from src.core.pipeline_context import StepContext
from src.core.logging_config import get_logger
from src.core.adls_uploader import get_uploader
from src.core.medallion_paths import step_logger_silver_dir
from src.core.colombian_time import current_colombian_time

logger = get_logger(__name__)


class StepLogger:
    def __init__(self, step_ctx: StepContext):
        self.step_ctx = step_ctx
        self.step_name = step_ctx.step_name
        self.run_id = step_ctx.run_id

        self.start_time = current_colombian_time()
        self.end_time = None

        self.input_count = 0
        self.output_count = 0
        self.status = "success"  # 'success', 'partial_success', 'failure'

        self.files_processed = []
        self.errors = []
        self.classification = {}
        self.costs = {"openai_tokens_total": 0, "ocr_pages": 0}
        self.step_metadata: Dict[str, Any] = {}

        self._uploader = get_uploader()
        if not self._uploader.initialized:
            try:
                from src.core.secret_helper import get_secret, SecretNames

                acct = get_secret(SecretNames.ADLS_STORAGE_NAME)
                self._uploader.initialize(account_name=acct, container_name="bronze")
            except Exception as e:
                logger.warning(f"[StepLogger] Auto-init of uploader failed: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.status = "failure"
            error_msg = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            self.errors.append(
                {
                    "step": self.step_name,
                    "file": "SYSTEM",
                    "message": f"Unhandled exception: {str(exc_val)}",
                }
            )
            logger.error(
                f"Step {self.step_name} failed with unhandled exception:\n{error_msg}"
            )

        self.finalize()
        # Return False to let the exception propagate
        return False

    def set_input_count(self, count: int):
        self.input_count = count

    def set_output_count(self, count: int):
        self.output_count = count

    def set_step_metadata(self, data: Dict[str, Any]) -> None:
        """Optional structured fields merged into the Silver JSON (e.g. SharePoint sync stats)."""
        self.step_metadata.update(data)

    def record_file(
        self,
        filename: str,
        status: str = "success",
        group: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Record the processing status of a single file/document."""
        entry = {"filename": filename, "status": status}
        if group:
            entry["group"] = group
            if status == "success":
                self.classification[group] = self.classification.get(group, 0) + 1

        if metadata:
            entry.update(metadata)

        if error:
            entry["error"] = error
            self.record_error(filename, error)

        self.files_processed.append(entry)

    def record_error(self, filename: str, message: str):
        """Record an error for a specific file."""
        if self.status != "failure":
            self.status = "partial_success"

        self.errors.append(
            {"step": self.step_name, "file": filename, "message": message}
        )

    def add_cost(self, ocr_pages: int = 0, openai_tokens: int = 0):
        self.costs["ocr_pages"] += ocr_pages
        self.costs["openai_tokens_total"] += openai_tokens

    def finalize(self):
        self.end_time = current_colombian_time()
        duration_seconds = (self.end_time - self.start_time).total_seconds()

        log_payload = {
            "step_name": self.step_name,
            "run_id": self.run_id,
            "status": self.status,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": duration_seconds,
            "input_count": self.input_count,
            "output_count": self.output_count,
            "files_processed": self.files_processed,  # No truncation, per user request
            "errors": self.errors,
            "classification": self.classification,
            "costs": self.costs,
        }
        if self.step_metadata:
            log_payload["step_metadata"] = dict(self.step_metadata)

        if self._uploader.enabled and self._uploader.service_client:
            parent_dir = step_logger_silver_dir(self.run_id, self.step_name)
            full_path = f"{parent_dir}/{self.step_name}.json"
            try:
                silver_client = self._uploader.service_client.get_file_system_client(
                    "silver"
                )
                self._uploader._ensure_directory_exists(parent_dir, silver_client)
                file_client = silver_client.get_file_client(full_path)
                json_bytes = json.dumps(
                    log_payload, ensure_ascii=False, indent=2
                ).encode("utf-8")
                file_client.upload_data(json_bytes, overwrite=True)
                logger.info(f"[StepLogger] Wrote step log to {full_path}")
            except Exception as e:
                logger.error(f"[StepLogger] Failed to write step log: {e}")
        else:
            logger.warning(
                "[StepLogger] Uploader not enabled, skipped writing ADLS log."
            )
