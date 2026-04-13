"""
ADLS Medallion Uploader Module
-------------------------------
Shared module for uploading data to Azure Data Lake Storage (ADLS) Gen2
in medallion architecture (Bronze/Silver/Gold layers).

This module provides a singleton uploader that can be used by all pipeline steps
to upload data inline during processing, reducing memory usage and enabling
real-time data visibility.

Usage:
    from adls_medallion_uploader import get_uploader

    uploader = get_uploader()
    uploader.initialize(account_name='myaccount', container_name='mycontainer')
    uploader.upload_to_bronze(local_path='file.pdf', remote_path='file.pdf', dir_path='Generic/')
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import threading

from src.core.medallion_paths import INDEXING_STEPS_ROOT, is_indexing_medallion_source

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import DefaultAzureCredential
    from azure.core.exceptions import ResourceExistsError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logging.warning("Azure SDK not available - ADLS uploads will be disabled")


class MedallionUploader:
    """
    Singleton uploader for medallion architecture.

    Provides thread-safe methods to upload files to Bronze, Silver, and Gold layers
    in ADLS Gen2 with automatic partitioning and error handling.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensure singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize uploader (called only once due to singleton)"""
        if not hasattr(self, "initialized"):
            self.service_client = None
            self.container_client = None
            self.initialized = False
            self.enabled = False
            self.config = {
                "use_date_partitioning": True,
                "partition_by_source": True,
                "async_upload": False,
                "retry_count": 3,
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
        """
        Initialize ADLS client.

        Args:
            account_name: ADLS storage account name
            container_name: Container name
            credential: Optional account key or connection string
            config: Optional configuration dictionary

        Returns:
            bool: True if initialization successful, False otherwise
        """
        if self.initialized:
            logging.info("✅ ADLS Medallion Uploader already initialized")
            return True

        if not AZURE_AVAILABLE:
            logging.warning("⚠️  Azure SDK not available - uploads disabled")
            self.enabled = False
            return False

        try:
            # Update config if provided
            if config:
                self.config.update(config)

            # Construct account URL
            account_url = f"https://{account_name}.dfs.core.windows.net"

            # Create service client with appropriate authentication
            if credential:
                self.service_client = DataLakeServiceClient(
                    account_url=account_url, credential=credential
                )
                logging.info(f"🔑 Using provided credential for {account_name}")
            else:
                self.service_client = DataLakeServiceClient(
                    account_url=account_url, credential=DefaultAzureCredential()
                )
                logging.info(f"🔐 Using DefaultAzureCredential for {account_name}")

            # Get or create container
            self.container_client = self.service_client.get_file_system_client(
                container_name
            )

            try:
                # Test access by checking if container exists
                self.container_client.get_file_system_properties()
                logging.info(f"✅ Connected to container: {container_name}")
            except Exception:
                # Try to create container if it doesn't exist
                try:
                    self.container_client.create_file_system()
                    logging.info(f"✅ Created container: {container_name}")
                except ResourceExistsError:
                    logging.info(f"✅ Container already exists: {container_name}")

            self.initialized = True
            self.enabled = True

            logging.info("✅ ADLS Medallion Uploader initialized successfully")
            logging.info(f"   Account: {account_name}")
            logging.info(f"   Container: {container_name}")
            logging.info(f"   Config: {self.config}")

            return True

        except Exception as e:
            logging.error(f"❌ Failed to initialize ADLS uploader: {e}")
            self.enabled = False
            self.stats["errors"].append(f"Initialization failed: {str(e)}")
            return False

    def _get_partition_path(
        self, layer: str, dir_path: str, source_step: Optional[str] = None
    ) -> str:
        """
        Generate partition path based on configuration.

        Args:
            layer: Layer name (bronze, silver, gold)
            dir_path: Directory path (e.g., 'Generic/')
            source_step: Optional source step for silver layer partitioning

        Returns:
            str: Full partition path
        """
        path_parts = []

        if layer == "silver" and is_indexing_medallion_source(source_step):
            path_parts.append(INDEXING_STEPS_ROOT)

        # Add source partition for silver layer if configured
        if layer == "silver" and self.config["partition_by_source"] and source_step:
            path_parts.append(f"source={source_step}")

        # Add date partitioning if configured
        if self.config["use_date_partitioning"]:
            now = datetime.now()
            path_parts.extend(
                [f"year={now.year}", f"month={now.month:02d}", f"day={now.day:02d}"]
            )

        # Add directory path
        if dir_path:
            path_parts.append(dir_path.rstrip("/"))

        return "/".join(path_parts)

    def upload_to_bronze(
        self, local_path: str, remote_path: str, dir_path: str
    ) -> bool:
        """
        Upload file to Bronze layer (raw data).

        Args:
            local_path: Local file path to upload
            remote_path: Remote file name/path within the partition
            dir_path: Directory path (e.g., 'Generic/')

        Returns:
            bool: True if upload successful, False otherwise
        """
        return self._upload_to_layer(
            layer="bronze",
            local_path=local_path,
            remote_path=remote_path,
            dir_path=dir_path,
        )

    def upload_to_silver(
        self, local_path: str, remote_path: str, dir_path: str, source_step: str
    ) -> bool:
        """
        Upload file to Silver layer (validated data).

        Args:
            local_path: Local file path to upload
            remote_path: Remote file name/path within the partition
            dir_path: Directory path (e.g., 'Generic/')
            source_step: Source step identifier (e.g., 'step2')

        Returns:
            bool: True if upload successful, False otherwise
        """
        return self._upload_to_layer(
            layer="silver",
            local_path=local_path,
            remote_path=remote_path,
            dir_path=dir_path,
            source_step=source_step,
        )

    def upload_to_gold(self, local_path: str, remote_path: str, dir_path: str) -> bool:
        """
        Upload file to Gold layer (enriched data).

        Args:
            local_path: Local file path to upload
            remote_path: Remote file name/path within the partition
            dir_path: Directory path (e.g., 'Generic/')

        Returns:
            bool: True if upload successful, False otherwise
        """
        return self._upload_to_layer(
            layer="gold",
            local_path=local_path,
            remote_path=remote_path,
            dir_path=dir_path,
        )

    def _upload_to_layer(
        self,
        layer: str,
        local_path: str,
        remote_path: str,
        dir_path: str,
        source_step: Optional[str] = None,
    ) -> bool:
        """
        Internal method to upload file to any layer.

        Args:
            layer: Layer name (bronze, silver, gold)
            local_path: Local file path
            remote_path: Remote file path
            dir_path: Directory path
            source_step: Optional source step for partitioning

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            if self.config["log_uploads"]:
                logging.debug(f"⏭️  Upload disabled, skipping {layer}/{remote_path}")
            return False

        try:
            # Generate full partition path
            partition_path = self._get_partition_path(layer, dir_path, source_step)
            full_path = f"{partition_path}/{remote_path}".replace("\\", "/")

            layer_client = self.service_client.get_file_system_client(layer)

            # Ensure directory exists
            self._ensure_directory_exists(partition_path, layer_client)

            # Upload file
            file_client = layer_client.get_file_client(full_path)

            if os.path.isfile(local_path):
                with open(local_path, "rb") as data:
                    file_client.upload_data(data, overwrite=True)

                # Update stats
                layer_key = f"{layer}_uploaded"
                self.stats[layer_key] = self.stats.get(layer_key, 0) + 1

                if self.config["log_uploads"]:
                    logging.info(f"📤 {layer.upper()}: {remote_path} → {full_path}")

                return True
            else:
                error_msg = f"File not found: {local_path}"
                logging.warning(f"⚠️  {error_msg}")
                self.stats["errors"].append(error_msg)
                return False

        except Exception as e:
            error_msg = f"Error uploading {local_path} to {layer}: {str(e)}"
            logging.error(f"❌ {error_msg}")
            self.stats["errors"].append(error_msg)
            return False

    def upload_json_data(
        self,
        json_data: Any,
        remote_path: str,
        dir_path: str,
        layer: str = "silver",
        source_step: Optional[str] = None,
    ) -> bool:
        """
        Upload JSON data directly without local file.

        Args:
            json_data: JSON-serializable data
            remote_path: Remote file name
            dir_path: Directory path
            layer: Target layer (default: silver)
            source_step: Optional source step for partitioning

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.enabled:
            return False

        try:
            # Generate full partition path
            partition_path = self._get_partition_path(layer, dir_path, source_step)
            full_path = f"{partition_path}/{remote_path}".replace("\\", "/")

            layer_client = self.service_client.get_file_system_client(layer)

            # Ensure directory exists
            self._ensure_directory_exists(partition_path, layer_client)

            # Upload JSON data
            file_client = layer_client.get_file_client(full_path)
            json_bytes = json.dumps(json_data, ensure_ascii=False, indent=2).encode(
                "utf-8"
            )
            file_client.upload_data(json_bytes, overwrite=True)

            # Update stats
            layer_key = f"{layer}_uploaded"
            self.stats[layer_key] = self.stats.get(layer_key, 0) + 1

            if self.config["log_uploads"]:
                logging.info(f"📤 {layer.upper()} (JSON): {remote_path} → {full_path}")

            return True

        except Exception as e:
            error_msg = f"Error uploading JSON to {layer}: {str(e)}"
            logging.error(f"❌ {error_msg}")
            self.stats["errors"].append(error_msg)
            return False

    def _ensure_directory_exists(self, directory_path: str, container_client=None):
        """
        Ensure directory structure exists in ADLS.

        Args:
            directory_path: Directory path to create
            container_client: Optional client to use (defaults to silver/gold or current layer)
        """
        if not directory_path:
            return
        try:
            client = container_client or self.container_client
            directory_client = client.get_directory_client(directory_path)
            directory_client.create_directory()
        except ResourceExistsError:
            pass  # Directory already exists
        except Exception as e:
            logging.debug(f"Directory creation skipped for {directory_path}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get upload statistics.

        Returns:
            dict: Statistics including upload counts and errors
        """
        return {
            **self.stats,
            "enabled": self.enabled,
            "initialized": self.initialized,
            "config": self.config,
        }

    def reset_stats(self):
        """Reset upload statistics."""
        self.stats = {
            "bronze_uploaded": 0,
            "silver_uploaded": 0,
            "gold_uploaded": 0,
            "errors": [],
        }
        logging.info("📊 Upload statistics reset")

    def disable(self):
        """Disable uploads (useful for testing or fallback)."""
        self.enabled = False
        logging.warning("⚠️  ADLS uploads disabled")

    def enable(self):
        """Enable uploads (if initialized)."""
        if self.initialized:
            self.enabled = True
            logging.info("✅ ADLS uploads enabled")
        else:
            logging.warning("⚠️  Cannot enable - uploader not initialized")


# Global uploader instance
_uploader = MedallionUploader()


def get_uploader() -> MedallionUploader:
    """
    Get the global medallion uploader instance.

    Returns:
        MedallionUploader: Singleton uploader instance
    """
    return _uploader


def initialize_from_args(args) -> bool:
    """
    Initialize uploader from argparse arguments.

    Uses Managed Identity via DefaultAzureCredential for ADLS authentication.
    Falls back to secret_helper for account name if not provided.

    Args:
        args: Argument namespace with ADLS configuration

    Returns:
        bool: True if initialization successful
    """
    uploader = get_uploader()

    # Check if medallion uploads are enabled
    medallion_enabled = (
        getattr(args, "ENABLE_MEDALLION_INLINE", "true").lower() == "true"
    )

    if not medallion_enabled:
        logging.info("ℹ️  Medallion inline uploads disabled by configuration")
        return False

    # Get configuration
    config = {
        "use_date_partitioning": getattr(args, "USE_DATE_PARTITIONING", "true").lower()
        == "true",
        "partition_by_source": getattr(args, "PARTITION_BY_SOURCE", "true").lower()
        == "true",
        "log_uploads": getattr(args, "LOG_MEDALLION_UPLOADS", "true").lower() == "true",
    }

    # Get account name from args or Key Vault
    account_name = getattr(args, "ADLS_ACCOUNT_NAME", None)
    container_name = getattr(args, "ADLS_CONTAINER_NAME", "bronze")

    # If account name not provided, try to get from Key Vault
    if not account_name:
        try:
            from secret_helper import get_secret, SecretNames

            account_name = get_secret(SecretNames.ADLS_STORAGE_NAME)
            logging.info("🔑 Retrieved ADLS account name from Key Vault")
        except Exception as e:
            logging.warning(f"⚠️  Could not get ADLS account name: {e}")
            return False

    # Initialize uploader with DefaultAzureCredential (no account key needed)
    # The uploader will use Managed Identity when credential=None
    logging.info("🔐 Using DefaultAzureCredential (Managed Identity) for ADLS")
    return uploader.initialize(
        account_name=account_name,
        container_name=container_name,
        credential=None,  # Forces DefaultAzureCredential
        config=config,
    )


# Convenience functions
def upload_bronze(local_path: str, remote_path: str, dir_path: str) -> bool:
    """Convenience function to upload to bronze layer."""
    return get_uploader().upload_to_bronze(local_path, remote_path, dir_path)


def upload_silver(
    local_path: str, remote_path: str, dir_path: str, source_step: str
) -> bool:
    """Convenience function to upload to silver layer."""
    return get_uploader().upload_to_silver(
        local_path, remote_path, dir_path, source_step
    )


def upload_gold(local_path: str, remote_path: str, dir_path: str) -> bool:
    """Convenience function to upload to gold layer."""
    return get_uploader().upload_to_gold(local_path, remote_path, dir_path)


if __name__ == "__main__":
    # Test module
    print("ADLS Medallion Uploader Module")
    print("==============================")
    print(f"Azure SDK Available: {AZURE_AVAILABLE}")

    uploader = get_uploader()
    print(f"Uploader Initialized: {uploader.initialized}")
    print(f"Uploader Enabled: {uploader.enabled}")
    print(f"Stats: {uploader.get_stats()}")
