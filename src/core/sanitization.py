"""
Sanitization Module
-------------------
Provides utilities to detect and redact personally identifiable
information (PII) from text before it is processed downstream.
It supports Azure Language-based detection with a regex fallback.
"""

import re
from typing import Optional
from .logging_config import get_logger
from .secret_helper import get_credential

logger = get_logger(__name__)

try:
    from azure.ai.textanalytics import TextAnalyticsClient

    AZURE_LANGUAGE_AVAILABLE = True
except ImportError:
    AZURE_LANGUAGE_AVAILABLE = False


class PIIFilter:
    def __init__(self, provider: str = "regex", endpoint: Optional[str] = None):
        self.provider = provider
        self.endpoint = endpoint
        self.client = None

        if provider == "azure_language":
            if not AZURE_LANGUAGE_AVAILABLE:
                logger.warning(
                    "azure-ai-textanalytics not installed, falling back to regex"
                )
                self.provider = "regex"
            elif not endpoint:
                logger.warning(
                    "No endpoint provided for Azure Language, falling back to regex"
                )
                self.provider = "regex"
            else:
                try:
                    credential = get_credential()
                    self.client = TextAnalyticsClient(
                        endpoint=endpoint, credential=credential
                    )
                    logger.info("Initialized Azure Language Client for PII")
                except Exception as e:
                    logger.warning(f"Failed to init Azure Language: {e}")
                    self.provider = "regex"

    def sanitize(self, text: str) -> str:
        if not text:
            return ""

        if self.provider == "azure_language" and self.client:
            return self._sanitize_azure(text)
        else:
            return self._sanitize_regex(text)

    def _sanitize_azure(self, text: str) -> str:
        try:
            # Simple sync call. For large scale, we should batch.
            # Using recognize_pii_entities
            response = self.client.recognize_pii_entities([text])[0]
            if response.is_error:
                return text

            redacted_text = response.redacted_text
            return redacted_text
        except Exception as e:
            logger.error(f"PII Sanitization Error: {e}")
            return text

    def _sanitize_regex(self, text: str) -> str:
        # Simple placeholders for common PII
        # Email
        text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "[EMAIL]", text)
        # Phone (simple)
        text = re.sub(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "[PHONE]", text)
        return text
