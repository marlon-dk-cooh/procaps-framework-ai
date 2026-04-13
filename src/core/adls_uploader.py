"""
ADLS Medallion Uploader Module
------------------------------
Shared utility for uploading ingestion and processing outputs to Azure
Data Lake Storage (ADLS) Gen2 following a medallion architecture
(Bronze/Silver/Gold layers). It centralizes connection management,
path conventions (date and source partitioning), and basic upload
statistics for downstream monitoring.
"""

import os
import json
import threading
from datetime import datetime
from typing import Any, Dict, Optional
from .logging_config import get_logger
from .medallion_paths import INDEXING_STEPS_ROOT, is_indexing_medallion_source

logger = get_logger(__name__)

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.core.exceptions import ResourceExistsError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logger.warning("Azure SDK not available - ADLS uploads will be disabled")


class MedallionUploader:
    """
    Singleton uploader for medallion architecture.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "initialized"):
            self.service_client = None
            self.container_client = None
            self.initialized = False
            self.enabled = False
            self.last_error = None
            self.config = {
                "use_date_partitioning": True,
                "partition_by_source": True,
                "log_uploads": True,
            }
            self.stats = {
                "bronze_uploaded": 0,
                "silver_uploaded": 0,
                "gold_uploaded": 0,
                "errors": [],
            }

    def initialize(
        self,
        account_name: str,
        container_name: str,
        credential: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if self.initialized:
            return True

        self.last_error = None

        if not AZURE_AVAILABLE:
            self.enabled = False
            return False

        try:
            if config:
                self.config.update(config)

            account_url = f"https://{account_name}.dfs.core.windows.net"

            if credential:
                self.service_client = DataLakeServiceClient(
                    account_url=account_url, credential=credential
                )
            else:
                from .secret_helper import get_credential

                self.service_client = DataLakeServiceClient(
                    account_url=account_url, credential=get_credential()
                )

            self.container_client = self.service_client.get_file_system_client(
                container_name
            )

            # Check existence or create
            try:
                self.container_client.get_file_system_properties()
            except Exception:
                try:
                    self.container_client.create_file_system()
                except ResourceExistsError:
                    pass

            self.initialized = True
            self.enabled = True
            logger.info(
                f"✅ ADLS Medallion Uploader initialized by account: {account_name}, container: {container_name}"
            )
            return True

        except Exception as e:
            self.last_error = str(e)
            if "AuthorizationFailure" in self.last_error:
                logger.error(
                    "❌ Failed to initialize ADLS uploader due to authorization failure. "
                    "Verify Storage Blob Data Contributor role for the job managed identity "
                    f"on storage account '{account_name}' and filesystem '{container_name}'. "
                    f"Details: {e}"
                )
            else:
                logger.error(f"❌ Failed to initialize ADLS uploader: {e}")
            self.enabled = False
            self.stats["errors"].append(f"Initialization failed: {str(e)}")
            return False

    def get_last_error(self) -> Optional[str]:
        return self.last_error

    def get_layer_client(self, layer: str):
        """Return the filesystem client for a medallion layer (e.g. 'silver', 'gold')."""
        if not self.service_client:
            return None
        return self.service_client.get_file_system_client(layer)

    def _get_partition_path(
        self, layer: str, dir_path: str, source_step: Optional[str] = None
    ) -> str:
        path_parts = []

        if layer == "silver" and is_indexing_medallion_source(source_step):
            path_parts.append(INDEXING_STEPS_ROOT)

        if layer == "silver" and self.config["partition_by_source"] and source_step:
            path_parts.append(f"source={source_step}")

        if self.config["use_date_partitioning"]:
            now = datetime.now()
            path_parts.extend(
                [
                    f"year={now.year}",
                    f"month={now.month:02d}",
                    f"day={now.day:02d}",
                ]
            )

        if dir_path:
            path_parts.append(dir_path.rstrip("/"))
        return "/".join(path_parts)

    def _upload_to_layer(
        self,
        layer: str,
        local_path: str,
        remote_path: str,
        dir_path: str,
        source_step: Optional[str] = None,
    ) -> bool:
        if not self.enabled:
            return False

        try:
            partition_path = self._get_partition_path(layer, dir_path, source_step)
            full_path = f"{partition_path}/{remote_path}".replace("\\", "/")

            layer_client = self.service_client.get_file_system_client(layer)

            # Ensure dir
            self._ensure_directory_exists(partition_path, layer_client)

            file_client = layer_client.get_file_client(full_path)

            if os.path.isfile(local_path):
                with open(local_path, "rb") as data:
                    file_client.upload_data(data, overwrite=True)

                self.stats[f"{layer}_uploaded"] += 1
                if self.config["log_uploads"]:
                    logger.info(f"📤 {layer.upper()}: {remote_path} -> {full_path}")
                return True
            else:
                logger.warning(f"File not found: {local_path}")
                return False

        except Exception as e:
            logger.error(f"Error uploading to {layer}: {e}")
            self.stats["errors"].append(str(e))
            return False

    def upload_to_bronze(
        self, local_path: str, remote_path: str, dir_path: str
    ) -> bool:
        return self._upload_to_layer("bronze", local_path, remote_path, dir_path)

    def upload_to_silver(
        self, local_path: str, remote_path: str, dir_path: str, source_step: str
    ) -> bool:
        return self._upload_to_layer(
            "silver", local_path, remote_path, dir_path, source_step=source_step
        )

    def upload_to_gold(self, local_path: str, remote_path: str, dir_path: str) -> bool:
        return self._upload_to_layer("gold", local_path, remote_path, dir_path)

    def upload_json_data(
        self,
        json_data: Any,
        remote_path: str,
        dir_path: str,
        layer: str = "silver",
        source_step: Optional[str] = None,
    ) -> bool:
        """Serialise *json_data* and upload directly to ADLS without a local temp file."""
        if not self.enabled:
            return False

        try:
            partition_path = self._get_partition_path(layer, dir_path, source_step)
            full_path = f"{partition_path}/{remote_path}".replace("\\", "/")

            layer_client = self.service_client.get_file_system_client(layer)
            self._ensure_directory_exists(partition_path, layer_client)

            file_client = layer_client.get_file_client(full_path)
            json_bytes = json.dumps(json_data, ensure_ascii=False, indent=2).encode(
                "utf-8"
            )
            file_client.upload_data(json_bytes, overwrite=True)

            self.stats[f"{layer}_uploaded"] = self.stats.get(f"{layer}_uploaded", 0) + 1
            if self.config["log_uploads"]:
                logger.info(f"📤 {layer.upper()} (JSON): {remote_path} -> {full_path}")
            return True

        except Exception as e:
            logger.error(f"Error uploading JSON to {layer}: {e}")
            self.stats["errors"].append(str(e))
            return False

    def _ensure_directory_exists(
        self, directory_path: str, container_client=None
    ) -> None:
        """Create the directory in ADLS if it does not already exist."""
        if not directory_path:
            return
        try:
            client = container_client or self.container_client
            client.get_directory_client(directory_path).create_directory()
        except Exception:
            pass  # ResourceExistsError or permission-to-create suppressed intentionally


def get_uploader() -> MedallionUploader:
    return MedallionUploader()
