"""Implementación de CosmosDB para metadata del pipeline."""
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient, ContainerProxy, DatabaseProxy
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from config.settings import settings
from src.utils.app_logger import get_logger
from src.core.file_helpers import (
    group_files_by_extension,
    proportion_by_file_group,
    ext_in_structured,
    ext_in_others,
)

logger = get_logger("🗄️ Inicializando módulo de CosmosDB")

# Partition key path usada al crear o consultar el contenedor.
PARTITION_KEY_PATH = "/id"
# 1.8 MB de umbral seguro para dejar margen a headers internos de Cosmos.
COSMOS_MAX_DOC_BYTES = 1_800_000

def build_metadata_document(
    doc_id: str,
    container: str,
    medallion: str,
    paths: list[str],
    file_sizes: dict[str, str] | None = None
) -> dict[str, Any]:
    """Construye el documento de metadatos con la estructura esperada por Cosmos.

    Args:
        container:   Nombre del contenedor de storage (e.g. 'azstapropdev').
        medallion:   Capa de medallion (e.g. 'bronze').
        paths:       Lista de rutas relativas listadas por DBFSMountPoint.
        file_sizes:  Dict {path: 'X.XX kB'} generado en el paso principal.
        doc_id:      ID del documento Cosmos. Si es None, se genera un ID con datetime.

    Returns:
        Documento listo para upsert en Cosmos DB.
    """
    grouped_paths   = group_files_by_extension(paths)
    len_per_group   = proportion_by_file_group(paths, return_pct=False)
    ext_structured  = ext_in_structured(grouped_paths)
    ext_other       = ext_in_others(grouped_paths)

    return {
        "id": doc_id,
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

    def __init__(
            self, 
            db: DatabaseProxy, 
            container_name: str, 
            doc_id: str | None
        ):
        self._db = db
        self._container_name = container_name
        self._container: ContainerProxy = db.get_container_client(container_name)
        self._doc_id = doc_id
        if self._doc_id is None:
            self._doc_id = f"meta-id-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')}"
        logger.info(
            "CosmosDB inicializado — db=%s, container=%s, doc_id=%s",
            db.id, container_name, self._doc_id
        )

    @staticmethod
    def _estimate_doc_size(document: Dict[str, Any]) -> int:
        """Estima el tamaño en bytes del documento serializado como JSON."""
        return sys.getsizeof(json.dumps(document, default=str))

    @staticmethod
    def _split_document(
        document: Dict[str, Any],
        storage_account: str,
        medallion: str,
        splittable_key: str,
    ) -> List[Dict[str, Any]]:
        """Divide un documento en partes más pequeñas particionando una clave grande.

        Toma el contenido de ``document[storage_account][medallion][splittable_key]``
        (que debe ser un ``dict``) y lo reparte en N documentos, cada uno con un
        subconjunto de las entradas.

        Args:
            document: Documento original que excede el límite.
            storage_account: Clave del storage account en el documento.
            medallion: Capa del datalake.
            splittable_key: Clave dentro de ``medallion`` cuyo valor (dict)
                se va a particionar (ej. ``"file_sizes"``, ``"grouped_path"``,
                ``"ocr_results"``).

        Returns:
            Lista de documentos parciales listos para upsert.
        """
        base_id = document["id"]
        medallion_data = document[storage_account][medallion]
        big_dict: dict = medallion_data.get(splittable_key, {})

        if not big_dict:
            return [document]

        # Construir un documento "esqueleto" sin la clave grande para medir overhead.
        skeleton = {
            k: v for k, v in document.items()
            if k not in (storage_account,)
        }
        skeleton[storage_account] = {
            medallion: {
                k: v for k, v in medallion_data.items()
                if k != splittable_key
            }
        }
        overhead = sys.getsizeof(json.dumps(skeleton, default=str))
        budget = COSMOS_MAX_DOC_BYTES - overhead

        # Repartir entradas del dict grande en chunks.
        parts: List[Dict[str, Any]] = []
        current_chunk: dict = {}
        current_size = 0

        for key, value in big_dict.items():
            entry_size = sys.getsizeof(json.dumps({key: value}, default=str))
            if current_chunk and (current_size + entry_size) > budget:
                parts.append(dict(current_chunk))
                current_chunk = {}
                current_size = 0
            current_chunk[key] = value
            current_size += entry_size

        if current_chunk:
            parts.append(current_chunk)

        # Construir documentos finales.
        result_docs: List[Dict[str, Any]] = []
        total_parts = len(parts)

        for idx, chunk in enumerate(parts, start=1):
            if idx == 1:
                # La primera parte preserva todo el resto del documento
                part_medallion = {
                    k: v for k, v in medallion_data.items() if k != splittable_key
                }
                part_medallion[splittable_key] = chunk
            else:
                # Las partes secundarias LLEVAN EXCLUSIVAMENTE su chunk (evitando saturacion 2MB)
                part_medallion = {splittable_key: chunk}

            part_doc = {
                **document, # Copia llaves root (que son inofensivas)
                "id": f"{base_id}_part-{idx}" if total_parts > 1 else base_id,
                "_part": idx,
                "_total_parts": total_parts,
            }
            # Sobreescribimos la clave especifica para no duplicar data
            part_doc[storage_account] = {
                medallion: part_medallion
            }
            
            result_docs.append(part_doc)

        return result_docs

    async def upsert_metadata(
        self,
        storage_account: str,
        medallion: str,
        document: Dict[str, Any],
        doc_id: str | None = None,
        splittable_key: str | None = None,
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """Actualiza (o crea) la metadata de un medallion dentro del documento
        asociado al storage account.

        Usa ``upsert`` de Cosmos, así que es idempotente: si el documento
        no existe lo crea; si ya existe lo sobreescribe.

        Si el documento excede el límite de 2 MB de Cosmos, se divide
        automáticamente usando ``splittable_key`` para particionar el
        campo más grande del documento.

        Args:
            storage_account: Nombre del storage account.
            medallion: Capa del datalake (``"bronze"``, ``"silver"``, ``"gold"``).
            document: Diccionario con la metadata del medallion.
            doc_id: ID del documento Cosmos o None para usar el doc_id por defecto.
            splittable_key: Clave dentro de ``medallion`` cuyo valor (dict)
                se particiona si el documento excede 2 MB.  Ej.
                ``"file_sizes"``, ``"ocr_results"``.  Si es None y el
                documento es demasiado grande, se intenta con
                ``"file_sizes"`` por defecto.

        Returns:
            El documento upsertado (o lista de documentos si se dividió).
        """
        if doc_id is None:
            doc_id = f"generated-meta-id-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

        doc_size = self._estimate_doc_size(document)

        # Si cabe en un solo documento, upsert directo.
        if doc_size <= COSMOS_MAX_DOC_BYTES:
            result = await self._container.upsert_item(body=document)
            logger.info(
                "Metadata upsertada — doc_id=%s, medallion=%s, tamaño=%.1f kB",
                doc_id, medallion, doc_size / 1024,
            )
            return result

        # Documento excede el límite → dividir.
        if splittable_key is None:
            splittable_key = "file_sizes"

        logger.warning(
            "⚠️ Documento %s excede el límite de Cosmos (%.1f kB > %.1f kB). "
            "Dividiendo por clave '%s'.",
            doc_id, doc_size / 1024, COSMOS_MAX_DOC_BYTES / 1024, splittable_key,
        )

        parts = self._split_document(
            document, storage_account, medallion, splittable_key
        )

        results = []
        for part_doc in parts:
            result = await self._container.upsert_item(body=part_doc)
            logger.info(
                "Metadata upsertada (parte %d/%d) — doc_id=%s, tamaño=%.1f kB",
                part_doc["_part"], part_doc["_total_parts"],
                part_doc["id"], self._estimate_doc_size(part_doc) / 1024,
            )
            results.append(result)

        return results

    async def get_metadata_by_id(
        self, doc_id: str | None = None
    ) -> Optional[Dict[str, Any]]:
        """Obtiene el documento de metadata. Si está dividido en partes, 
        lo reensambla automáticamente.

        Args:
            doc_id: ID del documento Cosmos.

        Returns:
            El documento completo (reensamblado si es necesario) o ``None``.
        """
        target_id = doc_id or self._doc_id
        
        try:
            # 1. Intentar leer el ID directamente (documentos no divididos)
            doc = await self._container.read_item(
                item=target_id, partition_key=target_id
            )
        except CosmosResourceNotFoundError:
            # 2. Si falla, intentar buscar la primera parte
            try:
                first_part_id = f"{target_id}_part-1"
                doc = await self._container.read_item(
                    item=first_part_id, partition_key=first_part_id
                )
            except CosmosResourceNotFoundError:
                logger.warning("No se encontró metadata para el id: %s", target_id)
                return None

        # 3. Si tiene partes, reensamblar
        if doc.get("_total_parts", 1) > 1:
            return await self._reassemble_document(target_id, doc)
        
        return doc

    async def _reassemble_document(
        self, base_id: str, first_part: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recupera todas las partes de un documento y las une."""
        total = first_part["_total_parts"]
        full_doc = first_part.copy()
        
        logger.info("🧩 Reensamblando documento %s (%d partes)...", base_id, total)
        
        for i in range(2, total + 1):
            part_id = f"{base_id}_part-{i}"
            part = await self._container.read_item(item=part_id, partition_key=part_id)
            self._merge_dicts(full_doc, part)
            
        # Limpiar metadatos de fragmentación
        full_doc.pop("_part", None)
        full_doc.pop("_total_parts", None)
        full_doc["id"] = base_id
        
        return full_doc

    def _merge_dicts(self, target: Dict[str, Any], source: Dict[str, Any]):
        """Une recursivamente diccionarios para recuperar las claves particionadas."""
        for k, v in source.items():
            if k in target and isinstance(target[k], dict) and isinstance(v, dict):
                self._merge_dicts(target[k], v)
            else:
                target[k] = v

    async def get_paths_by_group(
        self,
        storage_account: str,
        medallion: str,
        group: str,
        doc_id: str | None = None
    ) -> List[str]:
        """Devuelve las rutas de archivos para un grupo específico.

        Útil para que los pasos downstream obtengan, por ejemplo, solo
        los archivos ``textual`` o ``tabular`` sin tener que re-listar
        el storage.

        Args:
            doc_id: ID del documento Cosmos.
            storage_account: Nombre del storage (ej. "azstapropdev").
            medallion: Capa (``bronze`` / ``silver`` / ``gold``).
            group: Grupo de extensión según ``FILE_GROUPS``
                (``textual``, ``tabular``, ``images``, ``structured``, ``other``).

        Returns:
            Lista de rutas relativas.  Lista vacía si no se encuentra.
        """
        doc = await self.get_metadata_by_id(doc_id)
        if doc is None:
            return []

        storage_data = doc.get(storage_account, {})
        medallion_data = storage_data.get(medallion, {})
        return medallion_data.get("grouped_path", {}).get(group, [])

    async def get_file_sizes(
        self,
        doc_id: str,
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
        doc = await self.get_metadata_by_id(doc_id)
        if doc is None:
            return {}

        medallion_data = doc.get(medallion, {})
        return medallion_data.get("file_sizes", {})

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

def get_cosmos_client(
        endpoint:str = settings.azure_cosmos_endpoint, 
        api_key: str = settings.azure_cosmos_key
    ) -> CosmosClient:
    """Get a CosmosDB client."""
    try:
        client = CosmosClient(endpoint, api_key)
        logger.info(f"✅ Cliente de CosmosDB inicializado: {endpoint}")
        return client
    except Exception as e:
        logger.error(f"❌ No se pudo obtener el cliente de CosmosDB: {str(e)}")
        raise

def get_cosmos_database(
        client: CosmosClient,
        database_name: str = settings.azure_cosmos_database
    ) -> DatabaseProxy:
    """Get a CosmosDB database."""
    try:
        database = client.get_database_client(database_name)
        logger.info(f"✅ Base de datos de CosmosDB inicializada: {database_name}")
        return database
    except Exception as e:
        logger.error(f"❌ No se pudo obtener la base de datos de CosmosDB: {str(e)}")
        raise