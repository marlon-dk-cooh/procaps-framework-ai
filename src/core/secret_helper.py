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
from typing import Optional, List, Dict
from .logging_config import get_logger

logger = get_logger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

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
_scope_secret_cache: Dict[str, Dict[str, str]] = {}

# Default Key Vault URL - can be overridden via environment variable
DEFAULT_KEY_VAULT_URL = "https://azkvaprocapsdevservicios.vault.azure.net/"


def _env_or_default(env_name: str, default: str = "") -> str:
    """Resolve fallback values from environment first, then use a hardcoded default."""
    return os.getenv(env_name, default)


def _search_index_env_value(default: str = "") -> str:
    return (
        os.getenv("AZURE_SEARCH_INDEX")
        or os.getenv("AZURE_SEARCH_INDEX_NAME")
        or os.getenv("AZURE_SEARCH_INDEX_SEMANTIC")
        or default
    )


# Known configurations to bypass Key Vault if offline or permission denied
KNOWN_CONFIGS = {
    "azure-blob-storage-name": _env_or_default("AZURE_STORAGE_ACCOUNT_NAME", "stllmopsdll"),
    "azure-openai-endpoint": _env_or_default(
        "AZURE_OPENAI_ENDPOINT", "https://foundry-ia-llmops-dll.openai.azure.com/"
    ),
    # Empty keys force token-based (managed identity) auth in clients.
    "azure-openai-api-key": _env_or_default("AZURE_OPENAI_KEY", ""),
    "azure-form-recognizer-endpoint": _env_or_default(
        "AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT",
        "https://di-ia-llmops-dll.cognitiveservices.azure.com/",
    ),
    "azure-form-recognizer-api-key": _env_or_default(
        "AZURE_DOCUMENT_INTELLIGENCE_KEY", ""
    ),
    "openai-api-version": _env_or_default("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    "KV-AZSEARCH-Endpoint": _env_or_default(
        "AZURE_SEARCH_ENDPOINT", "https://srch-llmops-dll.search.windows.net"
    ),
    "KV-AZSEARCH-Key": _env_or_default("AZURE_SEARCH_KEY", ""),
    "KV-AZSEARCH-index": _search_index_env_value("rag-knowledge-base"),
    "model-llm-name": _env_or_default("AZURE_OPENAI_MODEL", "gpt-4o"),
    "model-embeddings-name": os.getenv(
        "AZURE_OPENAI_EMBEDDINGS_MODEL",
        os.getenv("AZURE_OPENAI_MODEL", "text-embedding-3-large"),
    ),
    "search-index-name": _search_index_env_value("rag-knowledge-base"),
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


def _get_dbutils():
    """
    Best-effort resolver for dbutils in Databricks runtime.
    Returns None when running outside Databricks.
    """
    try:
        import IPython

        ip = IPython.get_ipython()
        if ip is not None:
            dbutils = ip.user_ns.get("dbutils")
            if dbutils is not None:
                return dbutils
    except Exception:
        pass

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore

        return dbutils
    except Exception:
        pass

    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None


def _get_dbutils_secret_scope() -> Optional[str]:
    """
    Resolve Databricks secret scope name from environment.
    """
    return (
        os.getenv("DATABRICKS_SECRET_SCOPE")
        or os.getenv("DBUTILS_SECRET_SCOPE")
        or os.getenv("SECRET_SCOPE")
    )


def _normalize_scope_key(key: str) -> str:
    return key.replace("_", "-").strip().lower()


def _scope_key_candidates(secret_name: str) -> List[str]:
    candidates = [
        secret_name,
        secret_name.lower(),
        secret_name.replace("_", "-"),
        secret_name.replace("_", "-").lower(),
    ]
    # Preserve order, remove blanks/duplicates
    out: List[str] = []
    seen = set()
    for item in candidates:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _load_secrets_from_scope(scope: str) -> Dict[str, str]:
    """
    Load all secret names from a Databricks scope and materialize the values lazily once.

    Keys in the returned dict are normalized to UPPER_SNAKE_CASE for convenience:
    `azure-openai-key` -> `AZURE_OPENAI_KEY`
    """
    if scope in _scope_secret_cache:
        return dict(_scope_secret_cache[scope])

    dbutils = _get_dbutils()
    if dbutils is None:
        return {}

    secrets: Dict[str, str] = {}
    try:
        for meta in dbutils.secrets.list(scope):
            env_key = meta.key.replace("-", "_").upper()
            secrets[env_key] = dbutils.secrets.get(scope=scope, key=meta.key)
        logger.info(
            "Loaded %d secrets from Databricks scope '%s'.",
            len(secrets),
            scope,
        )
    except Exception as e:
        logger.warning("Could not load secrets from Databricks scope '%s': %s", scope, e)
        return {}

    _scope_secret_cache[scope] = dict(secrets)
    return dict(secrets)


def get_secret_with_dbutils(secret_name: str, scope: Optional[str] = None) -> str:
    """
    Read a secret from a Databricks secret scope, typically backed by Azure Key Vault.
    """
    dbutils = _get_dbutils()
    if dbutils is None:
        raise RuntimeError("dbutils is not available in this runtime")

    scope = scope or _get_dbutils_secret_scope()
    if not scope:
        raise ValueError(
            "Databricks secret scope not configured. Set DATABRICKS_SECRET_SCOPE, "
            "DBUTILS_SECRET_SCOPE, or SECRET_SCOPE."
        )

    last_error: Optional[Exception] = None
    for key in _scope_key_candidates(secret_name):
        try:
            value = dbutils.secrets.get(scope=scope, key=key)
            logger.info(
                "Retrieved secret '%s' from Databricks scope '%s' using key '%s'.",
                secret_name,
                scope,
                key,
            )
            return value
        except Exception as e:
            last_error = e

    # Final pass: inspect scope keys and resolve case/format differences.
    try:
        scope_secrets = _load_secrets_from_scope(scope)
        env_key = secret_name.replace("-", "_").upper()
        if env_key in scope_secrets:
            logger.info(
                "Retrieved secret '%s' from Databricks scope '%s' via normalized key '%s'.",
                secret_name,
                scope,
                env_key,
            )
            return scope_secrets[env_key]
    except Exception as e:
        last_error = e

    if last_error is not None:
        raise last_error
    raise KeyError(f"Secret '{secret_name}' was not found in Databricks scope '{scope}'")


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


def get_secret_with_managed_identity(
    secret_name: str, vault_url: Optional[str] = None
) -> str:
    """
    Get a secret from Key Vault using Managed Identity / DefaultAzureCredential.
    """
    client = get_secret_client(vault_url)
    secret = client.get_secret(secret_name)
    logger.info(f"🔑 Retrieved secret: {secret_name}")
    return secret.value


@lru_cache(maxsize=32)
def get_secret(secret_name: str, vault_url: Optional[str] = None) -> str:
    """
    Resolve a secret using the fastest available runtime path:
    1) Databricks secret scope (`dbutils`) when configured
    2) Direct Key Vault access through DefaultAzureCredential
    3) Local fallbacks from KNOWN_CONFIGS / .env
    """
    scope = _get_dbutils_secret_scope()
    if scope:
        try:
            return get_secret_with_dbutils(secret_name, scope=scope)
        except Exception as e:
            logger.warning(
                "Databricks scope lookup failed for %s in scope '%s': %s. "
                "Falling back to direct Key Vault access.",
                secret_name,
                scope,
                e,
            )

    try:
        return get_secret_with_managed_identity(secret_name, vault_url=vault_url)
    except Exception as e:
        if secret_name in KNOWN_CONFIGS:
            logger.warning(
                "Falling back to KNOWN_CONFIGS for %s after Key Vault lookup failed: %s",
                secret_name,
                e,
            )
            return KNOWN_CONFIGS[secret_name]
        raise


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
