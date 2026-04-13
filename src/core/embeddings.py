"""
Embeddings Client (OpenAI)
--------------------------
Provides a thin wrapper around Azure OpenAI's embeddings API to turn
lists of text chunks into numeric vector representations used for
semantic search and indexing within the workflow.
"""

from typing import List, Optional
from .logging_config import get_logger
from .secret_helper import get_openai_token_provider

logger = get_logger(__name__)

try:
    from openai import AzureOpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("openai package not available")


class EmbeddingsGenerator:
    def __init__(
        self,
        endpoint: str,
        deployment: str,
        api_version: str,
        key: Optional[str] = None,
    ):
        if not OPENAI_AVAILABLE:
            raise ImportError("openai package required")

        self.deployment = deployment

        if key:
            self.client = AzureOpenAI(
                azure_endpoint=endpoint, api_key=key, api_version=api_version
            )
        else:
            token_provider = get_openai_token_provider()
            self.client = AzureOpenAI(
                azure_endpoint=endpoint,
                azure_ad_token_provider=token_provider,
                api_version=api_version,
            )

    def generate(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        try:
            # Basic batching could go here, but AzureOpenAI handles lists.
            # Though heavy payload limits exist.
            response = self.client.embeddings.create(input=texts, model=self.deployment)
            return [data.embedding for data in response.data]
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            raise e
