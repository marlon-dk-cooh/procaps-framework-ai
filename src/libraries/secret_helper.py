"""
Secret Helper Module
--------------------
Replaces dbutils.secrets.get() with DefaultAzureCredential + SecretClient
for direct Key Vault access using Managed Identity.

Usage:
    from secret_helper import get_secret, get_credential

    # Get a secret from Key Vault
    api_key = get_secret("azure-openai-api-key")

    # Get credential for direct Azure SDK usage
    credential = get_credential()
"""

import os
import logging
from functools import lru_cache
from typing import Optional

try:
    from azure.identity import DefaultAzureCredential, get_bearer_token_provider
    from azure.keyvault.secrets import SecretClient

    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    AZURE_IDENTITY_AVAILABLE = False
    logging.warning("azure-identity not available - falling back to dbutils.secrets")


# Global cached instances
_credential = None
_secret_client = None

# Default Key Vault URL - can be overridden via environment variable
DEFAULT_KEY_VAULT_URL = "https://kv-llmops-dll.vault.azure.net/" # url de la Key Vault

# Known configurations to bypass Key Vault if offline or permission denied
# These are strictly for non-sensitive configuration values that might be blocked by 403s
KNOWN_CONFIGS = {
    "azure-blob-storage-name": "stllmopsdll",
    # Real endpoints found via 'az resource list'
    "azure-openai-endpoint": "https://foundry-ia-llmops-dll.openai.azure.com/", # url de Azure foundry
    "azure-form-recognizer-endpoint": "https://di-ia-llmops-dll.cognitiveservices.azure.com/", # url de Azure form recognizer
    "openai-api-version": "2024-08-01-preview", # version de la API de OpenAI
    # Mapped to SecretNames constants
    "KV-AZSEARCH-Endpoint": "https://srch-llmops-dll.search.windows.net", # url de Azure search
    # Additional mappings
    "model-llm-name": "gpt-4o", # nombre del modelo de LLM
    "model-embeddings-name": "text-embedding-ada-002", # nombre del modelo de embeddings
    "search-index-name": "rag-knowledge-base", # nombre del indice de search
}


def get_credential() -> "DefaultAzureCredential":
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
        _credential = DefaultAzureCredential()
        logging.info("Initialized DefaultAzureCredential (Managed Identity)")
    return _credential


def get_secret_client(vault_url: Optional[str] = None) -> "SecretClient":
    """
    Get cached SecretClient for Key Vault access.

    Args:
        vault_url: Optional Key Vault URL. Defaults to KEY_VAULT_URL env var or DEFAULT_KEY_VAULT_URL.
    """
    global _secret_client
    if _secret_client is None:
        if not AZURE_IDENTITY_AVAILABLE:
            raise ImportError("azure-keyvault-secrets package is required")

        vault_url = vault_url or os.environ.get("KEY_VAULT_URL", DEFAULT_KEY_VAULT_URL)
        _secret_client = SecretClient(vault_url=vault_url, credential=get_credential())
        logging.info(f"✅ Connected to Key Vault: {vault_url}")
    return _secret_client


@lru_cache(maxsize=32)
def get_secret(secret_name: str, vault_url: Optional[str] = None) -> str:
    """
    Get a secret from Key Vault using Managed Identity.

    This replaces: dbutils.secrets.get(scope="sc-autaiframeworkkv2", key="...")

    Args:
        secret_name: Name of the secret in Key Vault
        vault_url: Optional Key Vault URL override

    Returns:
        str: The secret value
    """
    # Check known configs first to bypass KV permissions for non-secrets
    if secret_name in KNOWN_CONFIGS:
        logging.info(f"ℹ️ using known config for: {secret_name}")
        return KNOWN_CONFIGS[secret_name]

    client = get_secret_client(vault_url)
    secret = client.get_secret(secret_name)
    logging.info(f"🔑 Retrieved secret: {secret_name}")
    return secret.value


def get_openai_token_provider():
    """
    Get a token provider for Azure OpenAI authentication.

    Use this instead of API key for OpenAI client:
        client = AzureOpenAI(azure_ad_token_provider=get_openai_token_provider(), ...)
    """
    if not AZURE_IDENTITY_AVAILABLE:
        raise ImportError("azure-identity package is required")

    return get_bearer_token_provider(
        get_credential(), "https://cognitiveservices.azure.com/.default"
    )


def get_secret_with_fallback(
    secret_name: str, fallback_scope: str = "kv-llmops-dll"
) -> str:
    """
    Get secret with fallback to dbutils.secrets if Managed Identity fails.

    This provides a graceful migration path.

    Args:
        secret_name: Name of the secret
        fallback_scope: Databricks secret scope for fallback

    Returns:
        str: The secret value
    """
    try:
        return get_secret(secret_name)
    except Exception as e:
        logging.warning(
            f"⚠️ MI secret retrieval failed for {secret_name}, falling back to dbutils: {e}"
        )
        try:
            # Attempt to use dbutils (available in Databricks runtime)
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils:
                logging.info(
                    f"Using dbutils to retrieve secret: scope={fallback_scope}, key={secret_name}"
                )
                try:
                    return dbutils.secrets.get(scope=fallback_scope, key=secret_name)
                except Exception as scope_error:
                    logging.error(
                        f"Failed to get secret from scope '{fallback_scope}': {scope_error}"
                    )
                    # Try to list available scopes to help debugging
                    try:
                        scopes = dbutils.secrets.listScopes()
                        scope_names = [s.name for s in scopes]
                        logging.info(f"ℹ️ Available Scopes: {scope_names}")
                        # If the fallback_scope is not in the list, try the first available one if it looks like a KV integration?
                        # No, that's too risky. Just log it.
                    except Exception as list_error:
                        logging.error(f"Could not list scopes: {list_error}")
                    raise scope_error
            else:
                logging.error("dbutils not found in user namespace")
        except Exception as dbutils_error:
            logging.error(
                f"❌ Fallback to dbutils also failed for {secret_name}: {dbutils_error}"
            )
            # If dbutils fails too, re-raise the original exception to show the root cause
            raise e
        # If we got here but didn't return, raise original error
        raise e


# Secret name constants matching Key Vault
class SecretNames:
    """Constants for Key Vault secret names"""

    # ADLS / Blob Storage
    ADLS_STORAGE_NAME = "azure-blob-storage-name"
    ADLS_STORAGE_KEY = "azure-blob-storage-key"
    ADLS_STORAGE_ENDPOINT = "azure-blob-storage-endpoint"
    ADLS_CONTAINER_NAME = "azure-blob-storage-container-name"

    # Azure OpenAI
    OPENAI_API_KEY = "azure-openai-api-key"
    OPENAI_ENDPOINT = "azure-openai-endpoint"
    OPENAI_API_VERSION = "openai-api-version"
    MODEL_LLM_NAME = "model-llm-name"
    MODEL_EMBEDDINGS_NAME = "model-embeddings-name"

    # Azure AI Search
    SEARCH_ENDPOINT = "KV-AZSEARCH-Endpoint"
    SEARCH_KEY = "KV-AZSEARCH-Key"
    SEARCH_INDEX = "KV-AZSEARCH-index"

    # Azure Form Recognizer (OCR)
    FORM_RECOGNIZER_API_KEY = "azure-form-recognizer-api-key"
    FORM_RECOGNIZER_ENDPOINT = "azure-form-recognizer-endpoint"

    # Cosmos DB
    COSMOS_ENDPOINT = "KV-CDB-FrameworkLLMOPS-Endpoint"
    COSMOS_KEY = "KV-CDB-FrameworkLLMOPS-Key"
    COSMOS_DATABASE = "KV-CDB-FrameworkLLMOPS-DatabaseName"

    SHAREPOINT_SITE_URL = "sharepoint-site-url"
    SHAREPOINT_CLIENT_ID = "sharepoint-client-id"
    SHAREPOINT_CLIENT_SECRET = "sharepoint-client-secret"
    SHAREPOINT_RELATIVE_PATH = "sharepoint-relative-path"


if __name__ == "__main__":
    # Test module
    print("Secret Helper Module")
    print("===================")
    print(f"Azure Identity Available: {AZURE_IDENTITY_AVAILABLE}")
    print(f"Default Key Vault URL: {DEFAULT_KEY_VAULT_URL}")
