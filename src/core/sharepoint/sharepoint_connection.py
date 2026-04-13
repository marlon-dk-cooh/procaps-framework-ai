import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse
import asyncio
import base64
from dataclasses import dataclass

from src.core.secret_helper import SecretNames, get_secret
from src.core.adls_uploader import MedallionUploader, get_uploader

try:
    from azure.core.exceptions import ResourceNotFoundError
except ImportError:
    ResourceNotFoundError = None  # type: ignore[misc, assignment]

UTC_OFFSET = "+00:00"


def _is_adls_not_found_error(exc: Exception) -> bool:
    if ResourceNotFoundError is not None and isinstance(exc, ResourceNotFoundError):
        return True
    name = type(exc).__name__
    return name == "ResourceNotFoundError" or "404" in str(exc).lower()

logger = logging.getLogger(__name__)


def normalize_sharepoint_site_url(url: str) -> str:
    """
    ClientContext debe apuntar a la raíz del sitio (p. ej. .../sites/NombreSitio),
    no a una biblioteca, carpeta ni a AllItems.aspx. Si en Key Vault se pegó la URL
    del navegador, se recorta al sitio.
    """
    raw = (url or "").strip()
    if not raw:
        return raw
    if not raw.lower().startswith(("http://", "https://")):
        raw = "https://" + raw.lstrip("/")
    parsed = urlparse(raw)
    path = parsed.path.rstrip("/")
    lower_path = path.lower()
    if "/forms/allitems.aspx" in lower_path:
        path = path[: lower_path.index("/forms/allitems.aspx")].rstrip("/")
    segments = [p for p in path.split("/") if p]
    if "sites" in segments:
        i = segments.index("sites")
        if i + 1 < len(segments):
            path = f"/sites/{segments[i + 1]}"
    elif "teams" in segments:
        i = segments.index("teams")
        if i + 1 < len(segments):
            path = f"/teams/{segments[i + 1]}"
    out = urlunparse(
        (parsed.scheme or "https", parsed.netloc, path or "/", "", "", "")
    )
    return out.rstrip("/")

try:
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.client_credential import ClientCredential
    SHAREPOINT_AVAILABLE = True
except ImportError as e:
    SHAREPOINT_AVAILABLE = False
    logger.error("❌ Error importando dependencias SharePoint: %s", e)
    raise ImportError(
        "Las dependencias de SharePoint no están disponibles. "
        "Instala Office365-REST-Python-Client."
    ) from e


@dataclass
class SharePointFileInfo:
    """Información completa de un archivo de SharePoint"""
    name: str
    url: str
    relative_path: str
    size: int
    modified: datetime
    extension: str
    folder_path: str
    content: Optional[str] = None
    content_type: Optional[str] = None

    def __post_init__(self):
        if not self.extension:
            self.extension = Path(self.name).suffix.lower().lstrip(".") if "." in self.name else ""


class SimpleSharePointClient:
    """Cliente simplificado para SharePoint."""

    def __init__(self, site_url: str, client_id: str, client_secret: str, relative_path: str):
        normalized = normalize_sharepoint_site_url(site_url)

        def _path_key(u: str) -> str:
            s = (u or "").strip()
            if not s.lower().startswith(("http://", "https://")):
                s = "https://" + s.lstrip("/")
            return urlparse(s).path.rstrip("/").lower()

        if _path_key(normalized) != _path_key(site_url or ""):
            logger.info(
                "site_url ajustada a la raíz del sitio (en KV use solo .../sites/Nombre): %r -> %r",
                site_url,
                normalized,
            )
        self.site_url = normalized
        self.client_id = client_id
        self.client_secret = client_secret
        self.relative_path = relative_path
        self._context = None
        self._last_connect_error: Optional[str] = None

    def _initialize_client(self) -> bool:
        """Inicializa el cliente SharePoint."""
        self._last_connect_error = None
        try:
            credentials = ClientCredential(self.client_id, self.client_secret)
            self._context = ClientContext(self.site_url).with_credentials(credentials)

            web = self._context.web.get().execute_query()
            logger.info("✅ Cliente SharePoint conectado a: %s", web.title)
            return True

        except Exception as e:
            self._last_connect_error = str(e)
            logger.error("❌ Error conectando a SharePoint: %s", e)
            return False

    def assert_authenticated(self) -> None:
        """
        Falla el paso si el token app-only no se puede obtener (no solo advertir y seguir).
        """
        if self._initialize_client():
            return
        detail = self._last_connect_error or "error desconocido"
        raise RuntimeError(
            f"SharePoint (app-only) no autenticó: {detail}. "
            "site_url debe ser la raíz del sitio (p. ej. https://tenant.sharepoint.com/sites/Nombre), "
            "no una URL de AllItems.aspx. Revise también permisos de aplicación en Entra ID "
            "(SharePoint/Graph, consentimiento admin) y que el client secret sea válido."
        )

    def _ensure_context_initialized(self) -> bool:
        if self._context or self._initialize_client():
            return True
        logger.error("❌ No se pudo inicializar el cliente SharePoint")
        return False

    def _limit_reached(self, max_files: int | None, items: list) -> bool:
        return bool(max_files and len(items) >= max_files)

    def _is_valid_subfolder(self, name: str) -> bool:
        return not name.startswith(".") and name not in ["Forms"]

    def _log_limit_stop(self, max_files: int) -> None:
        logger.info("🛑 Límite de %s archivos alcanzado, deteniendo exploración", max_files)

    def _file_info_if_relevant(self, file, folder_path: str) -> dict | None:
        props = file.properties
        name = props["Name"]
        extension = Path(name).suffix.lower().lstrip(".")

        if extension in ["pdf", "docx", "xlsx", "pptx", "txt", "doc", "xls", "ppt"]:
            info = {
                "name": name,
                "url": props["ServerRelativeUrl"],
                "relative_path": props["ServerRelativeUrl"],
                "size": props.get("Length", 0),
                "modified": props.get("TimeLastModified"),
                "folder_path": folder_path,
                "type": "file",
                "extension": extension,
            }
            logger.info("📄 Archivo encontrado: %s (%s bytes)", name, props.get("Length", 0))
            return info
        return None

    def _process_files_in_folder(
        self,
        folder,
        folder_path: str,
        max_files: int | None,
        all_items: list,
    ) -> None:
        for file in folder.files:
            try:
                if self._limit_reached(max_files, all_items):
                    self._log_limit_stop(max_files)
                    return

                info = self._file_info_if_relevant(file, folder_path)
                if info is not None:
                    all_items.append(info)

            except Exception as e:
                logger.warning("⚠️ Error procesando archivo en %s: %s", folder_path, e)
                continue

    def _process_subfolders_recursively(
        self,
        folder,
        folder_path: str,
        max_files: int | None,
        all_items: list,
    ) -> None:
        if self._limit_reached(max_files, all_items):
            return

        for subfolder in folder.folders:
            try:
                subfolder_name = subfolder.properties["Name"]
                if not self._is_valid_subfolder(subfolder_name):
                    continue

                subfolder_path = f"{folder_path}/{subfolder_name}"
                logger.info("📁 Entrando en subcarpeta: %s", subfolder_name)

                remaining_files = max_files - len(all_items) if max_files else None
                subfolder_items = self.explore_folder_recursively(subfolder_path, remaining_files)
                all_items.extend(subfolder_items)

                if self._limit_reached(max_files, all_items):
                    logger.info("🛑 Límite de %s archivos alcanzado", max_files)
                    break

            except Exception as e:
                logger.warning("⚠️ Error procesando subcarpeta: %s", e)
                continue

    def explore_folder_recursively(
        self,
        folder_path: str = None,
        max_files: int = None,
    ) -> List[Dict[str, Any]]:
        """Explora recursivamente carpetas y subcarpetas obteniendo archivos."""
        if not self._initialize_client():
            return []

        if folder_path is None:
            folder_path = self.relative_path

        all_items: List[Dict[str, Any]] = []

        try:
            folder = (
                self._context.web
                .get_folder_by_server_relative_url(folder_path)
                .expand(["Files", "Folders"])
                .get()
                .execute_query()
            )

            logger.info("🔍 Explorando carpeta: %s", folder_path)

            self._process_files_in_folder(folder, folder_path, max_files, all_items)

            if not max_files or len(all_items) < max_files:
                self._process_subfolders_recursively(folder, folder_path, max_files, all_items)

            return all_items

        except Exception as e:
            logger.error("❌ Error explorando carpeta %s: %s", folder_path, e)
            return []

    def _get_file_ref(self, file_url: str):
        logger.debug("📄 Obteniendo referencia del archivo: %s", file_url)
        return self._context.web.get_file_by_server_relative_url(file_url)

    def _download_file_bytes(self, file):
        import io

        logger.debug("📥 Descargando contenido del archivo...")
        output_stream = io.BytesIO()
        file.download(output_stream).execute_query()
        content_bytes = output_stream.getvalue()

        logger.debug("📦 Contenido descargado: %s bytes", len(content_bytes))
        if not content_bytes:
            logger.warning("⚠️ No se pudo descargar el contenido o el archivo está vacío")
            return None
        return content_bytes

    def _is_text_extension(self, ext: str) -> bool:
        return ext in [".txt", ".md", ".py", ".js", ".html", ".css", ".json", ".xml"]

    def _decode_text_or_base64(self, content_bytes: bytes) -> str | None:
        try:
            content = content_bytes.decode("utf-8")
            logger.info("✅ Archivo de texto decodificado exitosamente (%s caracteres)", len(content))
            return content
        except UnicodeDecodeError:
            try:
                content = content_bytes.decode("latin-1")
                logger.info("✅ Archivo decodificado con Latin-1 (%s caracteres)", len(content))
                return content
            except UnicodeDecodeError as e:
                logger.warning("⚠️ Error de decodificación, usando base64: %s", e)
                content = base64.b64encode(content_bytes).decode("ascii")
                logger.info("✅ Archivo convertido a base64 (%s caracteres)", len(content))
                return content

    def _to_base64_with_log(self, content_bytes: bytes, binary: bool = True) -> str:
        content = base64.b64encode(content_bytes).decode("ascii")
        if binary:
            logger.info("✅ Archivo binario convertido a base64 (%s caracteres)", len(content))
        else:
            logger.info("✅ Archivo convertido a base64 (%s caracteres)", len(content))
        return content

    def _handle_get_file_exception(self, e: Exception, file_url: str) -> None:
        error_msg = f"Error obteniendo contenido de {file_url}: {type(e).__name__}: {str(e)}"
        logger.error("❌ %s", error_msg)

        msg = str(e)
        if "Unauthorized" in msg or "Forbidden" in msg:
            logger.error("🔒 Error de permisos: Verifica que el usuario tenga acceso al archivo")
        elif "Not Found" in msg:
            logger.error("🔍 Archivo no encontrado: Verifica que la URL sea correcta")
        elif "Timeout" in msg:
            logger.error("⏱️ Timeout: El archivo puede ser muy grande o la conexión es lenta")
        else:
            logger.error("🔧 Error técnico detallado: %s", e)
        return None

    def get_file_content(self, file_url: str) -> Optional[str]:
        """Obtiene el contenido de un archivo de SharePoint."""
        logger.info("🔍 Intentando obtener contenido de: %s", file_url)

        if not self._ensure_context_initialized():
            return None

        try:
            file = self._get_file_ref(file_url)
            content_bytes = self._download_file_bytes(file)
            if not content_bytes:
                return None

            file_extension = Path(file_url).suffix.lower()
            logger.debug("📝 Procesando archivo con extensión: %s", file_extension)

            if self._is_text_extension(file_extension):
                return self._decode_text_or_base64(content_bytes)
            return self._to_base64_with_log(content_bytes, binary=True)

        except Exception as e:
            return self._handle_get_file_exception(e, file_url)

    def get_recent_files(self, days_back: int = 30) -> List[SharePointFileInfo]:
        """Obtiene archivos recientes de SharePoint."""
        if not self._initialize_client():
            return []

        since_date = datetime.now() - timedelta(days=days_back)
        files = []

        try:
            folder = (
                self._context.web
                .get_folder_by_server_relative_url(self.relative_path)
                .expand(["Files"])
                .get()
                .execute_query()
            )

            for file in folder.files:
                try:
                    props = file.properties
                    modified_str = props.get("TimeLastModified")

                    if modified_str:
                        modified = datetime.fromisoformat(
                            modified_str.replace("Z", UTC_OFFSET)
                        ).replace(tzinfo=None)

                        if modified > since_date:
                            file_info = SharePointFileInfo(
                                name=props["Name"],
                                url=props["ServerRelativeUrl"],
                                relative_path=props["ServerRelativeUrl"],
                                size=props.get("Length", 0),
                                modified=modified,
                                extension="",
                                folder_path=self.relative_path,
                            )

                            if file_info.extension in ["pdf", "docx", "xlsx", "pptx", "txt", "doc", "xls", "ppt"]:
                                files.append(file_info)

                except Exception as e:
                    logger.warning("⚠️ Error procesando archivo: %s", e)
                    continue

            logger.info("📄 Encontrados %s archivos recientes", len(files))
            return files

        except Exception as e:
            logger.error("❌ Error obteniendo archivos: %s", e)
            return []

    def list_folders(self) -> List[str]:
        """Lista las carpetas en la ruta configurada."""
        if not self._initialize_client():
            return []

        try:
            folder = (
                self._context.web
                .get_folder_by_server_relative_url(self.relative_path)
                .expand(["Folders"])
                .get()
                .execute_query()
            )

            folders = [
                f.properties["Name"]
                for f in folder.folders
                if not f.properties["Name"].startswith(".")
            ]

            logger.info("📂 Encontradas %s carpetas", len(folders))
            return folders

        except Exception as e:
            logger.error("❌ Error listando carpetas: %s", e)
            return []

    def close(self):
        """Cierra la conexión."""
        if self._context:
            self._context = None


class SharePointToBronzeProcess:
    """Proceso simplificado: SharePoint -> Bronze en Data Lake."""

    _CONN_KEYS = ("site_url", "client_id", "client_secret", "relative_path")

    @staticmethod
    def _empty_sync_stats() -> Dict[str, int]:
        return {
            "documents_synced": 0,
            "skipped_unchanged": 0,
            "skipped_since_date": 0,
            "failed": 0,
        }

    def __init__(self, sharepoint_config: Optional[Dict[str, Any]] = None):
        """
        sharepoint_config: dict opcional (típicamente config['sharepoint'] ya resuelto
        desde config.yaml vía load_config). Si los cuatro campos de conexión están
        definidos y resueltos, se usan; si no, se obtienen con get_secret (Key Vault).
        """
        self._sharepoint_config = sharepoint_config
        self.sharepoint_client: Optional[SimpleSharePointClient] = None
        self._skip_sync_if_unchanged = True
        self._sync_stats: Dict[str, int] = self._empty_sync_stats()
        self._initialize_sharepoint_client()

    @property
    def last_sync_stats(self) -> Dict[str, int]:
        """Contadores del último `run()` (archivos subidos, omitidos, fallidos)."""
        return dict(self._sync_stats)

    @staticmethod
    def _is_resolved_value(value: Any) -> bool:
        if value is None or value == "":
            return False
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            return False
        return True

    def _get_connection_params(self) -> tuple[str, str, str, str]:
        """Prioridad: config.yaml (sharepoint.*) resuelto; si no, secretos por nombre en Key Vault."""
        cfg = self._sharepoint_config
        if cfg:
            raw = {k: cfg.get(k) for k in self._CONN_KEYS}
            any_present = any(
                raw[k] not in (None, "") for k in self._CONN_KEYS
            )
            all_resolved = all(self._is_resolved_value(raw[k]) for k in self._CONN_KEYS)

            if all_resolved:
                return (
                    str(raw["site_url"]).strip(),
                    str(raw["client_id"]).strip(),
                    str(raw["client_secret"]).strip(),
                    str(raw["relative_path"]).strip(),
                )

            if any_present:
                raise ValueError(
                    "sharepoint en config: defina y resuelva los cuatro campos "
                    "(site_url, client_id, client_secret, relative_path) vía ${KV-...}, "
                    "o elimínelos para usar solo Key Vault."
                )

        return (
            get_secret(SecretNames.SHAREPOINT_SITE_URL),
            get_secret(SecretNames.SHAREPOINT_CLIENT_ID),
            get_secret(SecretNames.SHAREPOINT_CLIENT_SECRET),
            get_secret(SecretNames.SHAREPOINT_RELATIVE_PATH),
        )

    def _initialize_sharepoint_client(self) -> None:
        """Inicializa el cliente desde config resuelto o desde Key Vault."""
        try:
            site_url, client_id, client_secret, rel_path = self._get_connection_params()

            self.sharepoint_client = SimpleSharePointClient(
                site_url=site_url,
                client_id=client_id,
                client_secret=client_secret,
                relative_path=rel_path,
            )

            self.base_sp_path = rel_path.rstrip("/")
            self.root_folder = Path(self.base_sp_path).name
            self._bronze_root_name = self._compute_bronze_destination_folder()
            self._skip_sync_if_unchanged = self._parse_skip_sync_if_unchanged_flag()

            logger.info("✅ Cliente SharePoint inicializado exitosamente")

        except Exception as e:
            logger.error("❌ Error inicializando cliente SharePoint: %s", e)
            raise

    def _compute_bronze_destination_folder(self) -> str:
        """
        Carpeta (o prefijo) bajo el filesystem Bronze en ADLS. Por defecto: último
        segmento de relative_path; override con sharepoint.bronze_folder en config.
        """
        default = self.root_folder
        cfg = self._sharepoint_config or {}
        raw = cfg.get("bronze_folder")
        if raw is None:
            return default
        if isinstance(raw, str) and not self._is_resolved_value(raw):
            logger.warning(
                "sharepoint.bronze_folder no resuelto; usando nombre derivado de relative_path: %s",
                default,
            )
            return default
        s = str(raw).strip()
        if not s:
            return default
        s = s.replace("\\", "/").strip("/")
        parts = [p for p in s.split("/") if p and p != "."]
        if any(p == ".." for p in parts):
            raise ValueError("sharepoint.bronze_folder no puede contener segmentos '..'")
        if not parts:
            return default
        resolved = "/".join(parts)
        if resolved != default:
            logger.info(
                "Destino Bronze (sharepoint.bronze_folder): %s (por defecto hubiera sido: %s)",
                resolved,
                default,
            )
        return resolved

    def _parse_skip_sync_if_unchanged_flag(self) -> bool:
        """
        Si es True (por defecto), se evita descargar/subir cuando el blob en Bronze
        ya tiene la misma metadata sp_modified que SharePoint (archivo sin cambios).
        """
        cfg = self._sharepoint_config or {}
        v = cfg.get("skip_sync_if_unchanged")
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "y", "on")
        return bool(v)

    def _sp_modified_times_equal(self, stored: str, current: str) -> bool:
        """Compara fechas de modificación SharePoint (metadata) de forma robusta."""
        da = self._parse_modified_value(stored)
        db = self._parse_modified_value(current)
        if da and db:
            return self._to_utc_aware(da) == self._to_utc_aware(db)
        return stored.strip() == current.strip()

    def _blob_has_same_sp_revision(
        self,
        uploader: MedallionUploader,
        adls_dir: str,
        file_name: str,
        sp_mod_iso: str | None,
    ) -> bool:
        """True si el archivo en Bronze ya refleja la misma revisión que SharePoint."""
        if not self._skip_sync_if_unchanged or not sp_mod_iso:
            return False
        fs = uploader.container_client
        if fs is None:
            return False
        rel_path = f"{adls_dir}/{file_name}".replace("\\", "/")
        fc = fs.get_file_client(rel_path)
        try:
            props = fc.get_file_properties()
        except Exception as e:
            if _is_adls_not_found_error(e):
                return False
            logger.debug("No se pudieron leer propiedades de %s: %s", rel_path, e)
            return False
        meta = props.metadata or {}
        stored = None
        for k, val in meta.items():
            if str(k).lower() == "sp_modified":
                stored = val
                break
        if not stored:
            return False
        return self._sp_modified_times_equal(stored, sp_mod_iso)

    @staticmethod
    def _to_utc_aware(dt: datetime | None) -> datetime | None:
        """Devuelve dt como datetime aware en UTC."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _parse_modified_value(self, mod) -> datetime | None:
        """Parsea la fecha modified proveniente de SharePoint."""
        try:
            if isinstance(mod, str):
                if mod.endswith("Z"):
                    return datetime.fromisoformat(mod.replace("Z", UTC_OFFSET))
                return datetime.fromisoformat(mod)
            if isinstance(mod, datetime):
                return mod
            return None
        except Exception:
            return None

    def _should_skip_by_checkpoint(self, meta: dict, since_date: datetime | None) -> bool:
        """Omite archivos no modificados desde la fecha indicada."""
        if not since_date:
            return False

        since_utc = self._to_utc_aware(since_date)
        mod_dt = self._parse_modified_value(meta.get("modified"))
        mod_utc = self._to_utc_aware(mod_dt) if mod_dt else None

        logger.info(
            "Comparación de fechas: since=%s vs mod=%s",
            since_utc.isoformat(),
            mod_utc.isoformat() if mod_utc else None,
        )

        return bool(mod_utc and mod_utc <= since_utc)

    def _relative_paths(
        self,
        meta: dict,
        dest_root: str,
        *,
        strip_sp_root_segment: str | None = None,
    ):
        """Calcula la ruta destino en Bronze conservando la estructura relativa bajo relative_path.

        Si ``strip_sp_root_segment`` coincide con el primer segmento (p. ej. cuando ``only_folder``
        filtra una subcarpeta bajo ``relative_path``), ese segmento no se replica en ADLS: los
        archivos quedan bajo ``dest_root`` como si la raíz de ingesta fuera esa carpeta.
        """
        rel = meta["relative_path"].lstrip("/")
        base = self.base_sp_path.lstrip("/")
        rel = rel[len(base) :].lstrip("/")

        if strip_sp_root_segment:
            seg = str(strip_sp_root_segment).strip().strip("/")
            if seg:
                parts = [p for p in rel.split("/") if p]
                if parts and parts[0].lower() == seg.lower():
                    rel = "/".join(parts[1:])

        parent = Path(rel).parent
        parent_s = parent.as_posix()
        if parent_s in (".", ""):
            adls_dir = dest_root
        else:
            adls_dir = f"{dest_root}/{parent_s}"
        file_name = Path(rel).name
        return adls_dir, file_name

    async def _list_items_in_folder(self, sp_path: str):
        """Lista recursivamente archivos de una carpeta en SharePoint."""
        return await asyncio.to_thread(
            self.sharepoint_client.explore_folder_recursively,
            sp_path,
            None,
        )

    async def _load_content_from_sp(self, meta: dict):
        """Descarga el contenido de un archivo desde SharePoint."""
        content = await asyncio.to_thread(
            self.sharepoint_client.get_file_content,
            meta["url"],
        )

        if content is None:
            return None

        if isinstance(content, str) and meta.get("extension") not in {"txt", "md"}:
            return base64.b64decode(content)

        if isinstance(content, str):
            return content.encode()

        return content

    def _sp_modified_iso(self, meta: dict) -> str | None:
        """Obtiene la fecha de modificación de SharePoint en ISO."""
        try:
            sp_mod_val = meta.get("modified")
            sp_mod_dt = self._parse_modified_value(sp_mod_val)
            return sp_mod_dt.isoformat() if sp_mod_dt else None
        except Exception as e:
            logger.warning("⚠️ No se pudo parsear 'modified': %s → %s", meta.get("modified"), e)
            return None

    def _upload_to_adls_with_metadata(
        self,
        uploader: MedallionUploader,
        adls_dir: str,
        file_name: str,
        content: bytes,
        sp_mod_iso: str | None,
    ) -> None:
        """Sube un archivo al filesystem Bronze (ADLS Gen2) y guarda metadata básica."""
        fs = uploader.container_client
        if fs is None:
            raise RuntimeError("Cliente ADLS (bronze) no inicializado")

        rel_path = f"{adls_dir}/{file_name}".replace("\\", "/")
        uploader._ensure_directory_exists(adls_dir, fs)  # pylint: disable=protected-access
        file_client = fs.get_file_client(rel_path)
        file_client.upload_data(content, overwrite=True)

        try:
            if sp_mod_iso:
                file_client.set_metadata({"sp_modified": sp_mod_iso})
        except Exception as e:
            logger.warning("⚠️ No se pudo guardar metadata sp_modified=%s: %s", sp_mod_iso, e)

    def _select_roots(self, roots: list[str], only_folder: str | None, max_folders: int | None):
        """Selecciona qué carpetas raíz procesar."""
        if only_folder:
            filtered = [f for f in roots if f.lower() == only_folder.lower()]
            if not filtered:
                logger.error("❌ Carpeta raíz '%s' no encontrada", only_folder)
                return []
            return filtered

        if max_folders is not None:
            return roots[:max_folders]

        return roots

    async def _ingest_sharepoint_branch(
        self,
        sp_path: str,
        dest_root: str,
        uploader: MedallionUploader,
        since_date: datetime | None,
        *,
        strip_sp_root_segment: str | None = None,
    ) -> None:
        """Descarga recursivamente archivos bajo sp_path y los sube a Bronze."""
        items = await self._list_items_in_folder(sp_path)

        for meta in items:
            if self._should_skip_by_checkpoint(meta, since_date):
                self._sync_stats["skipped_since_date"] += 1
                continue

            try:
                adls_dir, file_name = self._relative_paths(
                    meta,
                    dest_root,
                    strip_sp_root_segment=strip_sp_root_segment,
                )
                sp_mod_iso = self._sp_modified_iso(meta)

                if await asyncio.to_thread(
                    self._blob_has_same_sp_revision,
                    uploader,
                    adls_dir,
                    file_name,
                    sp_mod_iso,
                ):
                    self._sync_stats["skipped_unchanged"] += 1
                    logger.info(
                        "⏭ Sin cambios en SharePoint (misma sp_modified en Bronze), omitido: %s/%s",
                        adls_dir,
                        file_name,
                    )
                    continue

                content = await self._load_content_from_sp(meta)

                if content is None:
                    self._sync_stats["failed"] += 1
                    logger.warning("⚠️ No se pudo descargar el archivo: %s", meta.get("url"))
                    continue

                self._upload_to_adls_with_metadata(
                    uploader,
                    adls_dir,
                    file_name,
                    content,
                    sp_mod_iso,
                )

                self._sync_stats["documents_synced"] += 1
                logger.info("✅ Archivo sincronizado en Bronze: %s/%s", adls_dir, file_name)

            except Exception as e:
                self._sync_stats["failed"] += 1
                logger.error("❌ Error procesando archivo %s: %s", meta.get("relative_path"), e)

    async def run(
        self,
        *,
        max_folders: int | None = None,
        only_folder: str | None = None,
        since_date: datetime | None = None,
    ) -> None:
        """
        Copia archivos desde SharePoint hacia Bronze en Data Lake.
        Con skip_sync_if_unchanged=true (config), solo descarga y sobrescribe si el
        blob no existe, no tiene sp_modified o la fecha difiere de SharePoint.
        """
        if self.sharepoint_client is None:
            raise RuntimeError("Cliente SharePoint no inicializado")

        self._sync_stats = self._empty_sync_stats()

        await asyncio.to_thread(self.sharepoint_client.assert_authenticated)

        uploader = get_uploader()
        if not uploader.initialized:
            uploader.initialize(
                account_name=get_secret(SecretNames.ADLS_STORAGE_NAME),
                container_name="bronze",
            )
        if not uploader.enabled:
            err = uploader.get_last_error() or "sin detalle"
            raise RuntimeError(
                "No se pudo inicializar ADLS para SharePoint→Bronze. "
                f"Comprueba cuenta, permisos MI y filesystem 'bronze'. Detalle: {err}"
            )

        # Mismo filesystem 'bronze' que el resto del workflow (sin prefijo duplicado bronze/).
        dest_root = self._bronze_root_name

        roots = await asyncio.to_thread(self.sharepoint_client.list_folders)
        roots = self._select_roots(roots, only_folder, max_folders)

        if roots:
            logger.info("📂 Subcarpetas bajo relative_path a procesar: %s", roots)
            strip_segment = only_folder if only_folder else None
            for folder in roots:
                sp_path = f"{self.base_sp_path}/{folder}"
                logger.info("📥 Procesando rama: %s", sp_path)
                await self._ingest_sharepoint_branch(
                    sp_path,
                    dest_root,
                    uploader,
                    since_date,
                    strip_sp_root_segment=strip_segment,
                )
        else:
            # Archivos suelen estar directamente en relative_path sin subcarpetas hijas.
            logger.info(
                "📂 Sin subcarpetas en relative_path; se procesa la carpeta raíz: %s",
                self.base_sp_path,
            )
            if only_folder:
                base_name = Path(self.base_sp_path).name
                if base_name.lower() != only_folder.lower():
                    logger.warning(
                        "only_folder=%r no coincide con el nombre de carpeta en relative_path (%r); "
                        "no se procesa nada.",
                        only_folder,
                        base_name,
                    )
                    return
            await self._ingest_sharepoint_branch(
                self.base_sp_path, dest_root, uploader, since_date
            )

        logger.info("🏁 Copia a Bronze terminada → %s", dest_root)

    def close(self) -> None:
        """Cierra el cliente de SharePoint."""
        if self.sharepoint_client:
            try:
                self.sharepoint_client.close()
                logger.info("🔒 Cliente SharePoint cerrado exitosamente")
            except Exception as e:
                logger.warning("⚠️ Error cerrando cliente SharePoint: %s", e)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    sharepoint_cfg = None
    try:
        repo_root = Path(__file__).resolve().parents[3]
        config_path = repo_root / "config" / "config.yaml"
        if config_path.is_file():
            from src.core.settings import load_config

            full_cfg = load_config(str(config_path))
            sharepoint_cfg = full_cfg.get("sharepoint")
    except Exception as e:
        logger.warning("No se pudo cargar config/config.yaml para main(): %s", e)

    async with SharePointToBronzeProcess(sharepoint_config=sharepoint_cfg) as process:
        await process.run(
            max_folders=None,
            only_folder=None,
            since_date=None,
        )


if __name__ == "__main__":
    asyncio.run(main())
