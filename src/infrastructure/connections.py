import asyncio
import json
import os
from typing import Any, List, Optional
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential
from azure.storage.filedatalake import DataLakeServiceClient
from src.core.models import AnalyzedDocument
from src.utils.app_logger import configure_logging, get_logger

logger = get_logger(__name__)

PATH_ERROR_MSG = "Error al obtener la ruta: %s"

class MountPoint:
    """Acceso a archivos en punto de montura.
    
    Args:
        root: Ruta raíz del mount point.
            Ej. ``"/mnt/bronce"`` o ``"./data"``.
        container: Opcional. Nombre del contenedor a usar como subruta base.
    """

    def __init__(self, root: str, container: str | None = None):
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
        self, document: bytes, file_type: str, model: str = "prebuilt-read"
    ) -> "AnalyzedDocument":
        """Analyze a document from raw bytes.

        Args:
            document: Raw file content bytes.
            file_type: Original file extension (e.g., "pdf").
            model: The DI model to use. Defaults to "prebuilt-read".

        Returns:
            An AnalyzedDocument holding the structured extraction results.
        """
        poller = self.client.begin_analyze_document(
            model, AnalyzeDocumentRequest(bytes_source=document)
        )
        result = poller.result()
        return self._build_result(result, file_type)

    def analyze_document_from_url(
        self, document_url: str, file_type: str, model: str = "prebuilt-read"
    ) -> "AnalyzedDocument":
        """Analyze a document from a public URL.

        Args:
            document_url: The public URL of the document.
            file_type: Expected file extension (e.g., "pdf").
            model: The DI model to use. Defaults to "prebuilt-read".

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
        
        

            

        
