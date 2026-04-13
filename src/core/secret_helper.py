"""
Secret Helper Module
--------------------
Replaces dbutils.secrets.get() with DefaultAzureCredential + SecretClient
for direct Key Vault access using Managed Identity.

Usage:
    from src.core.secret_helper import get_secret, get_credential

    # Get a secret from Key Vault
    api_key = get_secret("azure-openai-api-key")
"""

import os
from functools import lru_cache
from typing import Optional, List
from .logging_config import get_logger

logger = get_logger(__name__)

try:
    from azure.identity import DefaultAzureCredential, get_bearer_token_provider
    from azure.keyvault.secrets import SecretClient

    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    AZURE_IDENTITY_AVAILABLE = False
    logger.warning("azure-identity not available - falling back to dbutils.secrets")


# Global cached instances
_credential = None
_secret_client = None

# Default Key Vault URL - can be overridden via environment variable
DEFAULT_KEY_VAULT_URL = "https://kv-llmops-dll.vault.azure.net/"

# Known configurations to bypass Key Vault if offline or permission denied
KNOWN_CONFIGS = {
    "azure-blob-storage-name": "stllmopsdll",
    "azure-openai-endpoint": "https://foundry-ia-llmops-dll.openai.azure.com/",
    # Empty keys force token-based (managed identity) auth in clients.
    "azure-openai-api-key": "",
    "azure-form-recognizer-endpoint": "https://di-ia-llmops-dll.cognitiveservices.azure.com/",
    "azure-form-recognizer-api-key": "",
    "openai-api-version": "2024-08-01-preview",
    "KV-AZSEARCH-Endpoint": "https://srch-llmops-dll.search.windows.net",
    "KV-AZSEARCH-Key": "",
    "KV-AZSEARCH-index": "rag-knowledge-base",
    "model-llm-name": "gpt-4o",
    "model-embeddings-name": "text-embedding-3-large",
    "search-index-name": "rag-knowledge-base",
}


def _get_managed_identity_client_id() -> Optional[str]:
    """
    Resolve user-assigned managed identity client ID from environment.
    Prefer AZURE_MANAGED_IDENTITY_CLIENT_ID and fallback to AZURE_CLIENT_ID.
    """
    return os.environ.get("AZURE_MANAGED_IDENTITY_CLIENT_ID") or os.environ.get(
        "AZURE_CLIENT_ID"
    )


def _get_additionally_allowed_tenants() -> Optional[List[str]]:
    """
    Read optional cross-tenant configuration from environment.
    Expected format: comma-separated tenant IDs or '*'.
    """
    raw_value = os.environ.get("AZURE_ADDITIONALLY_ALLOWED_TENANTS", "").strip()
    if not raw_value:
        return None

    tenants = [tenant.strip() for tenant in raw_value.split(",") if tenant.strip()]
    return tenants or None


def get_credential():
    """
    Get cached DefaultAzureCredential instance.
    Uses Managed Identity when running on Databricks with Access Connector.
    """
    global _credential
    if _credential is None:
        if not AZURE_IDENTITY_AVAILABLE:
            raise ImportError(
                "azure-identity package is required. Install with: pip install azure-identity"
            )
        credential_kwargs = {}
        managed_identity_client_id = _get_managed_identity_client_id()
        if managed_identity_client_id:
            credential_kwargs["managed_identity_client_id"] = managed_identity_client_id
            logger.info(
                f"Using user-assigned managed identity client ID: {managed_identity_client_id}"
            )

        allowed_tenants = _get_additionally_allowed_tenants()
        if allowed_tenants:
            credential_kwargs["additionally_allowed_tenants"] = allowed_tenants
            logger.info(
                "Using AZURE_ADDITIONALLY_ALLOWED_TENANTS for cross-tenant auth"
            )

        # Force MI/CLI-based flows and ignore service-principal env credentials.
        _credential = DefaultAzureCredential(
            exclude_environment_credential=True,
            **credential_kwargs,
        )
        logger.info("Initialized DefaultAzureCredential (Managed Identity)")
    return _credential


def get_secret_client(vault_url: Optional[str] = None):
    """
    Get cached SecretClient for Key Vault access.
    """
    global _secret_client
    if _secret_client is None:
        if not AZURE_IDENTITY_AVAILABLE:
            raise ImportError("azure-keyvault-secrets package is required")

        # Backward-compatible env support:
        # prefer KEY_VAULT_URL, fallback to AZURE_KEY_VAULT_URL, else default.
        vault_url = (
            vault_url
            or os.environ.get("KEY_VAULT_URL")
            or os.environ.get("AZURE_KEY_VAULT_URL")
            or DEFAULT_KEY_VAULT_URL
        )
        _secret_client = SecretClient(vault_url=vault_url, credential=get_credential())
        logger.info(f"✅ Connected to Key Vault: {vault_url}")
    return _secret_client


@lru_cache(maxsize=32)
def get_secret(secret_name: str, vault_url: Optional[str] = None) -> str:
    """
    Get a secret from Key Vault using Managed Identity.
    """
    if secret_name in KNOWN_CONFIGS:
        logger.info(f"ℹ️ using known config for: {secret_name}")
        return KNOWN_CONFIGS[secret_name]

    client = get_secret_client(vault_url)
    secret = client.get_secret(secret_name)
    logger.info(f"🔑 Retrieved secret: {secret_name}")
    return secret.value


def get_openai_token_provider():
    """
    Get a token provider for Azure OpenAI authentication.
    """
    if not AZURE_IDENTITY_AVAILABLE:
        raise ImportError("azure-identity package is required")

    return get_bearer_token_provider(
        get_credential(), "https://cognitiveservices.azure.com/.default"
    )


class SecretNames:
    """Constants for Key Vault secret names"""

    ADLS_STORAGE_NAME = "azure-blob-storage-name"
    ADLS_STORAGE_KEY = "azure-blob-storage-key"
    ADLS_STORAGE_ENDPOINT = "azure-blob-storage-endpoint"
    ADLS_CONTAINER_NAME = "azure-blob-storage-container-name"
    OPENAI_API_KEY = "azure-openai-api-key"
    OPENAI_ENDPOINT = "azure-openai-endpoint"
    OPENAI_API_VERSION = "openai-api-version"
    MODEL_LLM_NAME = "model-llm-name"
    MODEL_EMBEDDINGS_NAME = "model-embeddings-name"
    SEARCH_ENDPOINT = "KV-AZSEARCH-Endpoint"
    SEARCH_KEY = "KV-AZSEARCH-Key"
    SEARCH_INDEX = "KV-AZSEARCH-index"
    FORM_RECOGNIZER_API_KEY = "azure-form-recognizer-api-key"
    FORM_RECOGNIZER_ENDPOINT = "azure-form-recognizer-endpoint"
    # SharePoint (fallback cuando config.sharepoint no trae los cuatro campos resueltos)
    SHAREPOINT_SITE_URL = "sharepoint-site-url"
    SHAREPOINT_CLIENT_ID = "sharepoint-client-id"
    SHAREPOINT_CLIENT_SECRET = "sharepoint-client-secret"
    SHAREPOINT_RELATIVE_PATH = "sharepoint-relative-path"
