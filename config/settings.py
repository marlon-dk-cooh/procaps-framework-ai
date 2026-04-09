"""Configuración centralizada del pipeline.

En Databricks, los valores se obtienen de un **Secret Scope** respaldado
por Azure Key Vault.  En desarrollo local se usa el archivo ``.env``
como fallback (pydantic-settings lo lee automáticamente).
"""
import os
from __future__ import annotations
from typing import Optional
from pydantic_settings import BaseSettings
from src.utils.app_logger import get_logger

logger = get_logger(__name__)

SECRET_SCOPE = "procaps-secrets"

def _get_dbutils():
    """Obtiene dbutils si estamos en un entorno Databricks."""
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except ImportError:
        pass
    return None

def _load_secrets_from_scope(scope: str) -> dict[str, str]:
    """Lee todos los secretos del scope y los devuelve como dict.

    Los nombres en Key Vault usan guiones (``azure-openai-key``),
    pero pydantic-settings espera guiones bajos (``azure_openai_key``).
    Esta función hace la conversión automáticamente.

    Returns:
        Dict ``{FIELD_NAME: value}`` con claves en MAYÚSCULAS y
        guiones bajos, listas para inyectar como variables de entorno.
        Dict vacío si no estamos en Databricks.
    """
    dbutils = _get_dbutils()
    if dbutils is None:
        return {}

    secrets: dict[str, str] = {}
    try:
        for meta in dbutils.secrets.list(scope):
            env_key = meta.key.replace("-", "_").upper()
            secrets[env_key] = dbutils.secrets.get(scope, meta.key)
        logger.info(
            "Se cargaron %d secretos desde el scope '%s'.",
            len(secrets), scope,
        )
    except Exception as e:
        logger.warning(
            "No se pudieron cargar secretos del scope '%s': %s", scope, e
        )
    return secrets

def _inject_secrets() -> None:
    """Inyecta los secretos del scope como variables de entorno.

    Esto permite que ``pydantic-settings`` los recoja de forma
    transparente, sin cambiar la definición de la clase ``Settings``.
    Solo sobreescribe variables que NO estén ya definidas, respetando
    cualquier override explícito vía env vars de cluster.
    """
    secrets = _load_secrets_from_scope(SECRET_SCOPE)
    for key, value in secrets.items():
        if key not in os.environ:
            os.environ[key] = value

_inject_secrets()

class Settings(BaseSettings):
    # Application settings.
    app_name: str = "Procaps Framework AI"
    app_version: str = "0.0.1"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    # Document Intelligence.
    azure_document_intelligence_endpoint: str = ""
    azure_document_intelligence_key: str = ""
    azure_document_intelligence_model: str = ""

    # OpenAI.
    azure_openai_endpoint: str = ""
    azure_openai_key: str = ""
    azure_openai_model: str = ""
    azure_openai_api_version: str = ""

    # Storage Account.
    azure_storage_account_name: str = ""
    azure_storage_account_key: str = ""
    azure_storage_account_container: str = ""

    # Cosmos DB.
    azure_cosmos_endpoint: str = ""
    azure_cosmos_key: str = ""
    azure_cosmos_database: str = ""
    azure_cosmos_container: str = ""

    # Azure AI Search.
    azure_search_endpoint: str = ""
    azure_search_key: str = ""
    azure_search_index: str = "procaps-index"

    # OpenAI Embeddings (separate deployment from chat model).
    azure_openai_embeddings_endpoint: str = ""
    azure_openai_embeddings_model: str = "text-embedding-3-large"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

    @property
    def llm_enabled(self) -> bool:
        return bool(
            self.azure_openai_endpoint
            and self.azure_openai_key
            and self.azure_openai_model
        )

settings = Settings()