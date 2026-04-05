"""Implementación de CosmosDB para metadata del pipeline."""
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient, ContainerProxy, DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from src.utils.app_logger import get_logger
from src.core.file_helpers import (
    group_files_by_extension,
    proportion_by_file_group,
    ext_in_structured,
    ext_in_others,
)

logger = get_logger(__name__)

# Partition key path usada al crear o consultar el contenedor.
PARTITION_KEY_PATH = "/partition_key"

def build_metadata_document(
    container: str,
    medallion: str,
    paths: list[str],
    file_sizes: dict[str, str] | None = None,
    doc_id: str | None = None,
) -> dict[str, Any]:
    """Construye el documento de metadatos con la estructura esperada por Cosmos.

    Args:
        container:   Nombre del contenedor de storage (e.g. 'azstapropdev').
        medallion:   Capa de medallion (e.g. 'bronze').
        paths:       Lista de rutas relativas listadas por DBFSMountPoint.
        file_sizes:  Dict {path: 'X.XX kB'} generado en el paso principal.
        doc_id:      ID del documento Cosmos. Si es None, se genera un UUID.

    Returns:
        Documento listo para upsert en Cosmos DB.
    """
    grouped_paths   = group_files_by_extension(paths)
    len_per_group   = proportion_by_file_group(paths, return_pct=False)
    ext_structured  = ext_in_structured(grouped_paths)
    ext_other       = ext_in_others(grouped_paths)

    return {
        "id": doc_id or str(uuid.uuid4()),
        container: {
            medallion: {
                "grouped_path":    grouped_paths,
                "len_per_group":   len_per_group,
                "ext_structured":  ext_structured,
                "ext_other":       ext_other,
                "file_sizes":      file_sizes,
            }
        },
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

class CosmosDB:
    """Cliente para leer/escribir metadata del pipeline en Cosmos DB.

    Cada documento representa un storage account y contiene la metadata
    generada por ``s00_load_files`` organizada por medallion
    (bronze / silver / gold).

    Args:
        db: Proxy de base de datos (``CosmosClient.get_database_client(...)``).
        container_name: Nombre del contenedor de Cosmos donde se almacena
            la metadata (ej. ``"pipeline_metadata"``).
    """

    def __init__(self, db: DatabaseProxy, container_name: str):
        self._db = db
        self._container_name = container_name
        self._container: ContainerProxy = db.get_container_client(container_name)
        logger.info(
            "CosmosDB inicializado — db=%s, container=%s",
            db.id, container_name,
        )

    async def upsert_metadata(
        self,
        storage_account: str,
        medallion: str,
        document: Dict[str, Any]

    ) -> Dict[str, Any]:
        """Actualiza (o crea) la metadata de un medallion dentro del documento
        asociado al storage account.

        Usa ``upsert`` de Cosmos, así que es idempotente: si el documento
        no existe lo crea; si ya existe lo sobreescribe.

        Args:
            storage_account: Nombre de la cuenta de almacenamiento
                (ej. ``"azstapropdev"``).  Se usa como ``id`` y ``partition_key``.
            medallion: Capa del datalake (``"bronze"``, ``"silver"``, ``"gold"``).
            document: Diccionario con la metadata del medallion.

        Returns:
            El documento upsertado tal como lo devuelve Cosmos.
        """
        result = await self._container.upsert_item(body=document)
        logger.info(
            "Metadata upsertada — account=%s, medallion=%s, archivos=%d",
            storage_account, medallion, sum(document[medallion]["len_per_group"].values()),
        )
        return result

    # ------------------------------------------------------------------
    # Lectura
    # ------------------------------------------------------------------

    async def get_metadata(
        self, storage_account: str
    ) -> Optional[Dict[str, Any]]:
        """Obtiene el documento completo de metadata para un storage account.

        Args:
            storage_account: Nombre del storage account (= partition key).

        Returns:
            El documento como ``dict``, o ``None`` si no existe.
        """
        try:
            item = await self._container.read_item(
                item=storage_account,
                partition_key=storage_account,
            )
            return item
        except CosmosResourceNotFoundError:
            logger.warning(
                "No se encontró metadata para account=%s", storage_account
            )
            return None

    async def get_paths_by_group(
        self,
        storage_account: str,
        medallion: str,
        group: str,
    ) -> List[str]:
        """Devuelve las rutas de archivos para un grupo específico.

        Útil para que los pasos downstream obtengan, por ejemplo, solo
        los archivos ``textual`` o ``tabular`` sin tener que re-listar
        el storage.

        Args:
            storage_account: Nombre del storage (= partition key).
            medallion: Capa (``bronze`` / ``silver`` / ``gold``).
            group: Grupo de extensión según ``FILE_GROUPS``
                (``textual``, ``tabular``, ``images``, ``structured``, ``other``).

        Returns:
            Lista de rutas relativas.  Lista vacía si no se encuentra.
        """
        doc = await self.get_metadata(storage_account)
        if doc is None:
            return []

        medallion_data = doc.get(medallion, {})
        return medallion_data.get("grouped_paths", {}).get(group, [])

    async def get_file_sizes(
        self,
        storage_account: str,
        medallion: str,
    ) -> Dict[str, str]:
        """Devuelve el diccionario ``{ruta: "N.N kB"}`` de tamaños.

        Pensado para reemplazar la lectura de ``file_sizes.json`` local
        en ``filter_by_size``.

        Args:
            storage_account: Nombre del storage (= partition key).
            medallion: Capa (``bronze`` / ``silver`` / ``gold``).

        Returns:
            Dict de tamaños.  Dict vacío si no se encuentra.
        """
        doc = await self.get_metadata(storage_account)
        if doc is None:
            return {}

        medallion_data = doc.get(medallion, {})
        return medallion_data.get("file_sizes", {})

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------

    async def ensure_container(self) -> None:
        """Crea el contenedor si no existe (idempotente).

        Útil en scripts de inicialización o la primera ejecución del
        pipeline.
        """
        await self._db.create_container_if_not_exists(
            id=self._container_name,
            partition_key=PartitionKey(path=PARTITION_KEY_PATH),
        )
        logger.info("Contenedor '%s' verificado/creado.", self._container_name)