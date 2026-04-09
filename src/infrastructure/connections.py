import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Optional
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SearchableField,
    SimpleField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
)
from src.core.models import AnalyzedDocument
from src.utils.app_logger import configure_logging, get_logger

logger = get_logger(__name__)

PATH_ERROR_MSG = "Error al obtener la ruta: %s"
DBFS_SCHEME = "dbfs:/"

class DBFSMountPoint:
    """Acceso a archivos en DBFS (Databricks File System).

    Reemplaza os.walk / open() / os.path.getsize por dbutils.fs.*
    manteniendo la misma interfaz pública que MountPoint.

    Args:
        root: Ruta raíz en DBFS.
            Ej. ``"dbfs:/mnt/bronce"`` o ``"/mnt/bronce"``.
        container: Opcional. Nombre del contenedor a usar como subruta base.
        dbutils: Instancia de dbutils (inyectada para facilitar tests).
            En un notebook de Databricks puedes pasar `dbutils` directamente.
    """

    def __init__(
        self,
        root: str = "dbfs:/mnt",
        container: str | None = None,
        medallion: str | None = None,
        dbutils=None,
    ):
        # Normaliza siempre al esquema dbfs:/ que entiende dbutils.fs
        self.root = self._normalize(root)
        self.container = container
        self.medallion = medallion
        self._dbutils = dbutils or self._get_dbutils()
        logger.info("Punto de montura DBFS inicializado en: %s", self.root)

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(path: str) -> str:
        """Garantiza el prefijo ``dbfs:/`` para todas las rutas."""
        if path.startswith("/dbfs/"):
            # /dbfs/mnt/... → dbfs:/mnt/...  (path local del driver)
            return DBFS_SCHEME + path[5:]
        if not path.startswith(DBFS_SCHEME):
            return DBFS_SCHEME + path.lstrip("/")
        return path

    @staticmethod
    def _to_local(dbfs_path: str) -> str:
        """
        Convierte ``dbfs:/foo`` → ``/dbfs/foo`` para poder usar
        open() nativo en el nodo driver (útil en read_file).
        """
        if dbfs_path.startswith(DBFS_SCHEME):
            return "/dbfs/" + dbfs_path[len(DBFS_SCHEME):]
        return dbfs_path

    @staticmethod
    def _get_dbutils():
        """
        Intenta obtener dbutils del contexto IPython (notebooks Databricks).
        Lanza un error claro si no está disponible.
        """
        try:
            import IPython
            ip = IPython.get_ipython()
            if ip and "dbutils" in ip.user_ns:
                return ip.user_ns["dbutils"]
        except ImportError:
            pass
        raise RuntimeError(
            "dbutils no está disponible. "
            "Ejecútalo en un notebook de Databricks o pásalo explícitamente: "
            "DBFSMountPoint(dbutils=dbutils)"
        )

    def get_path(self, directory: str) -> str:
        """Construye la ruta DBFS combinando root [+ container] + directory.

        Returns:
            Ruta con esquema ``dbfs:/`` lista para dbutils.fs.
        """
        if self.container is not None:
            base = f"{self.root}/{self.container}"
        elif self.container is not None and self.medallion is not None:
            base = f"{self.root}/{self.container}/{self.medallion}"
        else:
            base = self.root

        return f"{base}/{directory}".rstrip("/")

    def list_files(
        self, directory: str = "", max_workers: int = 10
    ) -> List[str]:
        """Lista recursivamente todos los archivos bajo un directorio en DBFS.

        Usa ThreadPoolExecutor para listar múltiples directorios en paralelo,
        ya que el cuello de botella es I/O de red (llamadas REST), no CPU.

        Args:
            directory: Sub-ruta relativa al root. ``""`` lista desde la raíz.
            max_workers: Número de hilos concurrentes (default 10).

        Returns:
            Lista de rutas relativas al root de los archivos encontrados.
        """
        try:
            base = self.get_path(directory)
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return []

        paths: List[str] = []
        dirs_to_explore = [base]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while dirs_to_explore:
                # Lanza todas las llamadas ls() en paralelo
                futures = {
                    executor.submit(self._dbutils.fs.ls, d): d
                    for d in dirs_to_explore
                }
                dirs_to_explore = []

                for future in as_completed(futures):
                    try:
                        entries = future.result()
                    except Exception as e:
                        logger.error(
                            "Error al listar %s: %s", futures[future], e
                        )
                        continue

                    for entry in entries:
                        if entry.name.endswith("/"):  # directorio
                            dirs_to_explore.append(entry.path)
                        else:  # archivo
                            rel = entry.path[len(base):].lstrip("/")
                            paths.append(rel)

        return paths

    def read_file(self, directory: str = "") -> bytes:
        """Lee el contenido completo de un archivo como bytes.

        Estrategia: usa ``open()`` sobre la ruta local ``/dbfs/...``
        (disponible en el nodo driver), que es el equivalente directo
        a la versión original con ``open(..., "rb")``.

        Para archivos muy grandes en workers usa ``spark.read`` en su lugar.

        Args:
            directory: Ruta relativa al root del archivo a leer.

        Returns:
            Contenido crudo como ``bytes``. Devuelve ``b""`` si hay error.
        """
        try:
            dbfs_path = self.get_path(directory)
            local_path = self._to_local(dbfs_path)  # /dbfs/mnt/...
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return b""

        try:
            # open() estándar funciona sobre /dbfs/ en el driver
            with open(local_path, "rb") as f:
                content = f.read()
            logger.info("Se leyeron %d bytes de %s", len(content), dbfs_path)
            return content
        except Exception as e:
            logger.error("Error al leer el archivo %s: %s", dbfs_path, e)
            return b""

    def get_file_size(self, directory: str = "") -> float:
        """Obtiene el tamaño de un archivo en kB **sin leerlo**.

        Sustituye ``os.path.getsize`` por ``dbutils.fs.ls``, que devuelve
        el campo ``FileInfo.size`` (en bytes) directamente desde el catálogo
        de DBFS sin transferir datos.

        Args:
            directory: Ruta relativa al root del archivo.

        Returns:
            Tamaño en kB. Devuelve ``0.0`` si hay error.
        """
        try:
            dbfs_path = self.get_path(directory)
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return 0.0

        try:
            # ls sobre un archivo individual devuelve una lista de 1 elemento
            entries = self._dbutils.fs.ls(dbfs_path)
            if not entries:
                raise FileNotFoundError(f"No existe: {dbfs_path}")
            return entries[0].size / 1024          # bytes → kB
        except Exception as e:
            logger.error(
                "Error al obtener tamaño del archivo %s: %s", dbfs_path, e
            )
            return 0.0

class MountPoint:
    """Acceso a archivos en punto de montura.
    
    Args:
        root: Ruta raíz del mount point.
            Ej. ``"/mnt/bronce"`` o ``"./data"``.
        container: Opcional. Nombre del contenedor a usar como subruta base.
    """

    def __init__(self, root: str = "./dbfs/mnt", container: str | None = None):
        self.root = root
        self.container = container
        logger.info("Punto de montura inicializado en: %s", self.root)

    def get_path(self, directory: str) -> str:
        """Construye la ruta absoluta combinando root + path relativo.

        Args:
            directory: Ruta relativa dentro del mount point.

        Returns:
            Ruta absoluta como string.
        """
        if self.container is not None:
            return os.path.join(self.root, self.container) + "/" + directory
        else:
            return os.path.join(self.root, directory)

    def list_files(self, directory: str = "") -> List[str]:
        """Lista recursivamente todos los archivos bajo un directorio.

        Equivalente a ``StorageAccount.list_files``, pero opera sobre el
        sistema de archivos local usando ``os.walk``.

        Args:
            directory: Sub-ruta relativa al root. Usa ``""`` para listar
                desde la raíz del mount point.

        Returns:
            Lista de rutas relativas al root de los archivos encontrados.
            Los directorios son excluidos del resultado.
        """
        try:
            base = self.get_path(directory)
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return []
        paths: List[str] = []
        try:
            for dirpath, _, filenames in os.walk(base):
                for filename in filenames:
                    abs_path = os.path.join(dirpath, filename)
                    rel_path = os.path.relpath(abs_path, base)
                    paths.append(rel_path)
        except Exception as e:
            logger.error("Error al listar archivos en %s: %s", base, e)
        return paths

    def read_file(self, directory: str = "") -> bytes:
        """Lee el contenido completo de un archivo como bytes.

        Equivalente a ``StorageAccount.read_file``, pero usa ``open()``
        en lugar del SDK de Azure.

        Args:
            directory: Ruta relativa al root del archivo a leer.
                Ej. ``"raw/invoices/invoice_001.pdf"``.

        Returns:
            Contenido crudo del archivo como ``bytes``.
            Devuelve ``b""`` si hay un error.
        """
        try:
            base = self.get_path(directory)
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return b""
        
        # Si el directorio es valido.
        try:
            with open(base, "rb") as f:
                content = f.read()
            logger.info("Se leyeron %d bytes de %s", len(content), base)
            return content
        except Exception as e:
            logger.error("Error al leer el archivo %s: %s", base, e)
            return b""

    def get_file_size(self, directory: str = "") -> float:
        """Obtiene el tamaño de un archivo en kilobytes (kB) sin leerlo.

        Equivalente a ``StorageAccount.get_file_size``.

        Args:
            directory: Ruta relativa al root del archivo.

        Returns:
            Tamaño en kB. Devuelve ``0.0`` si hay un error.
        """
        try:
            base = self.get_path(directory)
        except Exception as e:
            logger.error(PATH_ERROR_MSG, e)
            return 0.0

        # Si el directorio es valido.
        try:
            return os.path.getsize(base) / 1024
        except Exception as e:
            logger.error(
                "Error al obtener tamaño del archivo %s: %s", base, e
            )
            return 0.0

    def write_file(self, output_path: str, content: bytes = b"") -> None:
        """Escribe bytes en una ruta dada, creando directorios si no existen.

        Equivalente a ``StorageAccount.write_file``, pero opera localmente.

        Args:
            output_path: Ruta relativa al root donde se escribirá el archivo.
                Ej. ``"processed/results/output.json"``.
            content: Contenido en bytes a escribir. Default: ``b""``.
        """
        abs_path = self.get_path(output_path)
        try:
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            with open(abs_path, "wb") as f:
                f.write(content)
            logger.info("Archivo escrito correctamente en %s", abs_path)
        except Exception as e:
            logger.error("Error al escribir el archivo %s: %s", abs_path, e)


class DocumentIntelligenceConnection:
    """Client for analyzing documents using Azure Document Intelligence.
    
    Extracts text, paragraphs, tables, and basic metadata from various document
    formats (PDF, docx, xlsx, pptx, images).
    """

    def __init__(self, endpoint: str, key: str):
        self.client = DocumentIntelligenceClient(endpoint, AzureKeyCredential(key))
        logger.info("DocumentIntelligenceConnection initialized.")

    def analyze_document_from_stream(
        self, document: bytes, file_type: str, model: str = "prebuilt-layout"
    ) -> "AnalyzedDocument":
        """Analyze a document from raw bytes.

        Args:
            document: Raw file content bytes.
            file_type: Original file extension (e.g., "pdf").
            model: The DI model to use. Defaults to "prebuilt-layout".

        Returns:
            An AnalyzedDocument holding the structured extraction results.
        """
        poller = self.client.begin_analyze_document(
            model, AnalyzeDocumentRequest(bytes_source=document)
        )
        result = poller.result()
        return self._build_result(result, file_type)

    def analyze_document_from_url(
        self, document_url: str, file_type: str, model: str = "prebuilt-layout"
    ) -> "AnalyzedDocument":
        """Analyze a document from a public URL.

        Args:
            document_url: The public URL of the document.
            file_type: Expected file extension (e.g., "pdf").
            model: The DI model to use. Defaults to "prebuilt-layout".

        Returns:
            An AnalyzedDocument holding the structured extraction results.
        """        
        poller = self.client.begin_analyze_document(
            model, AnalyzeDocumentRequest(url_source=document_url)
        )
        result = poller.result()
        return self._build_result(result, file_type)

    def count_pages(self, document: bytes, model: str = "prebuilt-read") -> int:
        """Contar numero de paginas en un documento dado."""
        poller = self.client.begin_analyze_document(
            model, AnalyzeDocumentRequest(bytes_source=document)
        )
        result = poller.result()
        return len(result.pages)

    def _build_result(self, result: Any, file_type: str) -> "AnalyzedDocument":
        """Map the Azure SDK AnalyzeResult into our AnalyzedDocument dataclass."""

        # Extract paragraphs safely
        paragraphs = [p.content for p in result.paragraphs] if result.paragraphs else []
        
        # Extract tables safely (grid of cells)
        tables_data = []
        if result.tables:
            for table in result.tables:
                # Initialize grid for the table
                row_count = table.row_count
                col_count = table.column_count
                grid = [["" for _ in range(col_count)] for _ in range(row_count)]
                for cell in table.cells:
                    grid[cell.row_index][cell.column_index] = cell.content
                tables_data.append(grid)
                
        # #TODO: Asignar metadata.
        metadata = {
            "model_id": getattr(result, "model_id", "unknown"),
            "languages": [lang.locale for lang in (result.languages or [])],
            "topic": None, 
        }

        return AnalyzedDocument(
            content=result.content or "",
            pages=len(result.pages) if result.pages else 0,
            file_type=file_type,
            paragraphs=paragraphs,
            tables=tables_data,
            metadata=metadata,
        )

class StorageAccount:

    def __init__(self, account_name: str, account_key: str):
        self._client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net", 
            credential=account_key,
            api_version="2026-02-06" # #TODO: Review this version.
            )
        logger.info("StorageAccount initialized for %s", account_name)

    def list_files(self, container: str, directory: str = "/") -> List[str]:
        """List file paths inside a directory of a container.

        Args:
            container: Name of the file system (container) in the storage account.
            directory: Path of the directory to list. Defaults to root ``"/"``.

        Returns:
            A list of file paths (strings) found under the given directory.
            Directories themselves are excluded from the result.
        """
        try:
            file_system_client = self._client.get_file_system_client(file_system=container)
            paths = file_system_client.get_paths(path=directory)
        except Exception as e:
            logger.error(f"Error al listar archivos en el contenedor {container}: {e}")
            return []
        return [
            path.name
            for path in paths
            if not path.is_directory
        ]

    def read_file(self, container: str, file_path: str, timeout: int = 30) -> bytes:
        """
        Lee el contenido completo de un archivo como bytes.

        Args:
            container: Nombre del sistema de archivos (contenedor).
            file_path: Ruta completa del archivo dentro del contenedor
                (ej. ``"raw/invoices/invoice_001.pdf"``).
            timeout: Tiempo máximo en segundos para la descarga. Default: 30s.

        Returns:
            The raw bytes of the file content.
        """
        try:
            file_system_client = self._client.get_file_system_client(file_system=container)
            file_client = file_system_client.get_file_client(file_path)
            download = file_client.download_file(timeout=timeout)
            content = download.readall()
            logger.info("Se leyeron %d bytes de %s/%s", len(content), container, file_path)
        except Exception as e:
            logger.error(f"Error al leer el archivo {file_path} en el contenedor {container}: {e}")
            return b""
        return content

    def get_file_size(self, container: str, file_path: str) -> float:
        """
        Obtiene el tamaño de un archivo en kilobytes (kB) sin descargarlo.

        Args:
            container: Nombre del sistema de archivos (contenedor).
            file_path: Ruta completa del archivo dentro del contenedor.

        Returns:
            El tamaño del archivo en kB. Devuelve 0.0 si hay un error.
        """
        try:
            file_system_client = self._client.get_file_system_client(file_system=container)
            file_client = file_system_client.get_file_client(file_path)
            properties = file_client.get_file_properties()
            return properties.size / 1024
        except Exception as e:
            logger.error(f"Error al obtener tamaño del archivo {file_path} en contenedor {container}: {e}")
            return 0.0

    def write_file(self, container: str, output_path: str) -> None: # r
        """
        Escribe el procesamiento de un paso en una ruta especifica.

        Args:
            container: Nombre del sistema de archivos (contenedor).
            output_path: Ruta del directorio de salida dentro del contenedor
                (ej. ``"raw/invoices/"``).
        """
        try:
            file_system_client = self._client.get_file_system_client(file_system=container)
            file_client = file_system_client.get_file_client(output_path)
            if file_client.exists():
                logger.info("El directorio %s ya existe, se omite la creación.", output_path)
            else:
                file_system_client.create_directory(output_path)
                logger.info("Directorio %s creado correctamente.", output_path)
        except Exception as e:
            logger.error(f"Error al crear el directorio {output_path} en el contenedor {container}: {e}")


class AzureSearchConnection:
    """Client para gestión de índices y documentos en Azure AI Search.

    Encapsula la creación de índices con soporte vectorial (HNSW) y
    la subida de documentos en batches usando upsert (merge_or_upload).

    Args:
        endpoint: URL del servicio Azure AI Search.
        key: Clave de administración del servicio.
        index_name: Nombre del índice a crear/usar.
    """

    ALGORITHM_NAME = "hnsw-config"
    PROFILE_NAME = "vector-profile"

    def __init__(self, endpoint: str, key: str, index_name: str):
        self._credential = AzureKeyCredential(key)
        self._endpoint = endpoint
        self.index_name = index_name
        self._index_client = SearchIndexClient(endpoint, self._credential)
        self._search_client = SearchClient(endpoint, index_name, self._credential)
        logger.info(
            "AzureSearchConnection inicializado: endpoint=%s, index=%s",
            endpoint, index_name,
        )

    def create_or_update_index(self, vector_dimensions: int = 3072) -> None:
        """Crea o actualiza el índice con soporte de búsqueda vectorial.

        Esquema del índice:
            - ``id`` — clave primaria, filtrable.
            - ``origin`` — ruta original, searchable y filtrable.
            - ``content`` — texto completo, searchable.
            - ``metadata`` — JSON string, searchable.
            - ``update_at`` — timestamp ISO-8601, filtrable y sortable.
            - ``contentVector`` — vector HNSW, coseno.

        Args:
            vector_dimensions: Dimensiones del vector de embedding.
                Default: 3072 (text-embedding-3-large).
        """
        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(name=self.ALGORITHM_NAME),
            ],
            profiles=[
                VectorSearchProfile(
                    name=self.PROFILE_NAME,
                    algorithm_configuration_name=self.ALGORITHM_NAME,
                ),
            ],
        )

        fields = [
            SimpleField(
                name="id", type=SearchFieldDataType.String,
                key=True, filterable=True,
            ),
            SearchableField(
                name="origin", type=SearchFieldDataType.String,
                filterable=True,
            ),
            SearchableField(
                name="content", type=SearchFieldDataType.String,
            ),
            SearchableField(
                name="metadata", type=SearchFieldDataType.String,
            ),
            SimpleField(
                name="update_at", type=SearchFieldDataType.String,
                filterable=True, sortable=True,
            ),
            SearchField(
                name="contentVector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=vector_dimensions,
                vector_search_profile_name=self.PROFILE_NAME,
            ),
        ]

        index = SearchIndex(
            name=self.index_name,
            fields=fields,
            vector_search=vector_search,
        )

        self._index_client.create_or_update_index(index)
        logger.info(
            "✅ Índice '%s' creado/actualizado (%d dimensiones).",
            self.index_name, vector_dimensions,
        )

    def upload_documents(
        self, documents: List[dict], batch_size: int = 100,
    ) -> dict:
        """Sube documentos al índice en batches usando merge_or_upload (upsert).

        Args:
            documents: Lista de diccionarios con los campos del índice.
            batch_size: Documentos por batch (max recomendado: 1000).

        Returns:
            Resumen ``{succeeded: int, failed: int, errors: list}``.
        """
        total = len(documents)
        succeeded = 0
        failed = 0
        errors: List[str] = []

        for start in range(0, total, batch_size):
            batch = documents[start : start + batch_size]
            batch_num = (start // batch_size) + 1
            logger.info(
                "📤 Subiendo batch %d (%d docs, offset %d/%d)...",
                batch_num, len(batch), start, total,
            )
            try:
                results = self._search_client.merge_or_upload_documents(documents=batch)
                for r in results:
                    if r.succeeded:
                        succeeded += 1
                    else:
                        failed += 1
                        errors.append(f"{r.key}: {r.error_message}")
                        logger.error(
                            "❌ Documento %s falló: %s", r.key, r.error_message
                        )
            except Exception as e:
                failed += len(batch)
                errors.append(f"Batch {batch_num}: {e}")
                logger.error("❌ Batch %d falló: %s", batch_num, e)

        summary = {"succeeded": succeeded, "failed": failed, "errors": errors}
        logger.info(
            "🏁 Upload completado: %d exitosos, %d fallidos de %d total.",
            succeeded, failed, total,
        )
        return summary
