"""
Azure AI Search Client
----------------------
Provides a thin wrapper around Azure AI Search document operations.
It connects to an existing index and uploads documents in batches
for ingestion into the search service.
"""

from typing import List, Dict, Any, Optional
from .logging_config import get_logger
from .secret_helper import get_credential

logger = get_logger(__name__)

try:
    from azure.search.documents import SearchClient as AzSearchClient
    from azure.core.credentials import AzureKeyCredential

    SEARCH_AVAILABLE = True
except ImportError:
    SEARCH_AVAILABLE = False


class SearchClient:
    def __init__(self, endpoint: str, index_name: str, key: Optional[str] = None):
        if not SEARCH_AVAILABLE:
            raise ImportError("azure-search-documents package required")

        self.endpoint = endpoint
        self.index_name = index_name

        if key:
            self.credential = AzureKeyCredential(key)
        else:
            self.credential = get_credential()

        self.client = AzSearchClient(
            endpoint=endpoint, index_name=index_name, credential=self.credential
        )
        logger.info(f"Initialized Search Client for index: {index_name}")

    def upload_documents(
        self, documents: List[Dict[str, Any]], batch_size: int = 1000
    ) -> int:
        """
        Upload documents in batches.
        """
        if not documents:
            return 0

        total = len(documents)
        success_count = 0

        for i in range(0, total, batch_size):
            batch = documents[i : i + batch_size]
            try:
                results = self.client.upload_documents(documents=batch)
                # results is a list of IndexingResult
                succeeded = sum(1 for r in results if r.succeeded)
                success_count += succeeded

                if succeeded < len(batch):
                    errors = [r.error_message for r in results if not r.succeeded]
                    logger.warning(
                        f"Batch partial failure: {len(errors)} errors. First: {errors[0]}"
                    )

            except Exception as e:
                logger.error(f"Batch upload failed: {e}")
                # Continue? Or raise?
                # Raising ensures we don't silently fail large chunks
                raise e

        return success_count

    def list_document_ids(self, batch_size: int = 1000) -> List[str]:
        """
        Return all existing document IDs in the current index.
        """
        ids: List[str] = []
        skip = 0

        while True:
            results = self.client.search(
                search_text="*",
                select=["id"],
                top=batch_size,
                skip=skip,
            )
            batch_ids = [r.get("id") for r in results if r and r.get("id")]
            ids.extend(batch_ids)

            if len(batch_ids) < batch_size:
                break
            skip += batch_size

        logger.info("Fetched %s existing IDs from index: %s", len(ids), self.index_name)
        return ids

    def delete_documents_by_ids(self, ids: List[str], batch_size: int = 1000) -> int:
        """
        Delete documents by ID in batches.
        Returns number of successful deletions.
        """
        if not ids:
            logger.info("No obsolete documents to delete in index: %s", self.index_name)
            return 0

        success_count = 0
        total = len(ids)

        for i in range(0, total, batch_size):
            batch_ids = ids[i : i + batch_size]
            payload = [{"id": doc_id} for doc_id in batch_ids]
            try:
                results = self.client.delete_documents(documents=payload)
                succeeded = sum(1 for r in results if r.succeeded)
                success_count += succeeded
                if succeeded < len(batch_ids):
                    errors = [r.error_message for r in results if not r.succeeded]
                    logger.warning(
                        "Delete batch partial failure: %s errors. First: %s",
                        len(errors),
                        errors[0] if errors else "unknown error",
                    )
            except Exception as e:
                logger.error("Delete batch failed: %s", e)
                raise e

        logger.info(
            "Deleted %s/%s documents from index: %s",
            success_count,
            total,
            self.index_name,
        )
        return success_count
