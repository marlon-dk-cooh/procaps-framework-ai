"""
Azure AI Search Index Manager
-----------------------------
Ensures an Azure AI Search index exists before document ingestion.
If the index is missing, it creates it with the fields, semantic
configuration, and vector search settings required by the workflow.
"""

from typing import Optional
from .logging_config import get_logger
from .secret_helper import get_credential

logger = get_logger(__name__)

try:
    from azure.core.credentials import AzureKeyCredential
    from azure.core.exceptions import ResourceExistsError
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import (
        SearchIndex,
        SimpleField,
        SearchField,
        ComplexField,
        SearchFieldDataType,
        SemanticConfiguration,
        SemanticPrioritizedFields,
        SemanticField,
        SemanticSearch,
        VectorSearch,
        HnswAlgorithmConfiguration,
        VectorSearchProfile,
    )

    SEARCH_INDEX_AVAILABLE = True
except ImportError:
    SEARCH_INDEX_AVAILABLE = False


def ensure_index_exists(
    endpoint: str, index_name: str, vector_dimensions: int, key: Optional[str] = None
) -> None:
    """Create the Azure Search index if it does not already exist."""

    if not SEARCH_INDEX_AVAILABLE:
        raise ImportError("azure-search-documents package required for index creation.")

    if key:
        credential = AzureKeyCredential(key)
    else:
        credential = get_credential()

    index_client = SearchIndexClient(endpoint=endpoint, credential=credential)

    try:
        # Check if it already exists to avoid unnecessary create exceptions when possible
        # (Though create_index handles ResourceExistsError, it's cleaner to check if list_index_names is fast)
        existing_indices = [name for name in index_client.list_index_names()]
        if index_name in existing_indices:
            logger.info(f"Index already exists: {index_name}. Skipping creation.")
            return
    except Exception as e:
        logger.warning(
            f"Could not list indices (permissions?), proceeding to try-create: {e}"
        )

    # Define fields for a richer schema with nested metadata and semantic config.
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SearchField(
            name="content",
            type=SearchFieldDataType.String,
            searchable=True,
            analyzer_name="standard.lucene",
        ),
        ComplexField(
            name="metadata",
            fields=[
                SimpleField(
                    name="count_characters",
                    type=SearchFieldDataType.Int32,
                    filterable=True,
                    sortable=True,
                ),
                # SimpleField(
                #     name="count_tokens",
                #     type=SearchFieldDataType.Int32,
                #     filterable=True,
                #     sortable=True,
                # ),
                SearchField(
                    name="source",
                    type=SearchFieldDataType.String,
                    filterable=True,
                    searchable=True,
                ),
                SearchField(
                    name="type_source",
                    type=SearchFieldDataType.String,
                    filterable=True,
                    searchable=True,
                ),
            ],
        ),
        SearchField(
            name="keywords",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            searchable=True,
        ),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=vector_dimensions,
            vector_search_profile_name="myHnswProfile",
        ),
    ]

    # Configure Vector Search Config (required for vector fields)
    vector_search = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name="myHnsw")],
        profiles=[
            VectorSearchProfile(
                name="myHnswProfile",
                algorithm_configuration_name="myHnsw",
            )
        ],
    )

    semantic_config = SemanticConfiguration(
        name="my-semantic-config",
        prioritized_fields=SemanticPrioritizedFields(
            title_field=SemanticField(field_name="metadata/source"),
            keywords_fields=[SemanticField(field_name="keywords")],
            content_fields=[SemanticField(field_name="content")],
        ),
    )
    semantic_search = SemanticSearch(configurations=[semantic_config])

    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search,
    )

    try:
        logger.info(
            f"Attempting to create index: {index_name} with {vector_dimensions} dimensions..."
        )
        index_client.create_index(index)
        logger.info(f"Created index: {index_name} successfully.")
    except ResourceExistsError:
        logger.warning(
            f"Index {index_name} already existed (caught via ResourceExistsError)."
        )
    except Exception as e:
        logger.error(f"Failed to create index {index_name}: {e}")
        # Re-raise because if we need the index and it couldn't be created, indexing will fail anyway.
        raise
