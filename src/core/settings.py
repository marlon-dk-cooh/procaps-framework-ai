"""
Settings
--------
Provides a singleton-based configuration layer for the workflow that
loads `config.yaml` and YAML manifiestos (ruta por step; el de indexación
es `config/indexing_manifest.yaml`), y resuelve secretos (p. ej. `${KV-...}`)
vía `secret_helper` para que otros módulos accedan a la configuración ya resuelta.
"""

import yaml
from typing import Dict, Any
from .logging_config import get_logger
from .secret_helper import get_secret

logger = get_logger(__name__)


class Settings:
    """
    Singleton class to manage configuration loading and secret resolution.
    """

    _instance = None
    _config: Dict[str, Any] = {}
    _manifest: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
        return cls._instance

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and parse config.yaml, resolving secrets."""
        if self._config:
            return self._config

        logger.info(f"Loading config from {config_path}")
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)

        self._config = self._resolve_secrets(raw_config)
        return self._config

    def load_manifest(self, manifest_path: str) -> Dict[str, Any]:
        """Load and parse a manifest YAML (path is passed by each step's CLI)."""
        if self._manifest:
            return self._manifest

        logger.info(f"Loading manifest from {manifest_path}")
        with open(manifest_path, "r") as f:
            self._manifest = yaml.safe_load(f)

        return self._manifest

    def _resolve_secrets(self, config: Any) -> Any:
        """Recursively resolve strings like ${KV-Secret-Name} using secret_helper."""
        if isinstance(config, dict):
            return {k: self._resolve_secrets(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_secrets(i) for i in config]
        elif (
            isinstance(config, str)
            and config.startswith("${KV-")
            and config.endswith("}")
        ):
            secret_name = config[5:-1]  # Extract name between ${KV- and }
            try:
                # Primary lookup without KV- prefix to keep compatibility with
                # existing KNOWN_CONFIGS and legacy secret names.
                val = get_secret(secret_name)
                return val if val is not None else config
            except Exception as e:
                # Fallback: some vaults store names with the KV- prefix.
                prefixed_secret_name = f"KV-{secret_name}"
                try:
                    val = get_secret(prefixed_secret_name)
                    logger.info(
                        "Resolved secret using KV- prefixed name: %s",
                        prefixed_secret_name,
                    )
                    return val if val is not None else config
                except Exception as prefixed_e:
                    logger.warning(f"Failed to resolve secret {secret_name}: {e}")
                    logger.warning(
                        "Fallback with prefixed name %s also failed: %s",
                        prefixed_secret_name,
                        prefixed_e,
                    )
                    return config
        else:
            return config


# Global instance
settings = Settings()


def load_config(path: str) -> Dict[str, Any]:
    return settings.load_config(path)


def load_manifest(path: str) -> Dict[str, Any]:
    return settings.load_manifest(path)
