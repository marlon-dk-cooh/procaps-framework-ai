"""
Azure Document Intelligence (OCR) Client
---------------------------------------
Provides a wrapper around Azure Document Intelligence to extract
plain text from PDF and image files using the prebuilt OCR model.
It is used by the workflow to turn scanned or non-text documents
into text that can be processed by downstream steps.
"""

import io
from typing import Optional, Any, Union
from .logging_config import get_logger
from .secret_helper import get_credential

logger = get_logger(__name__)

try:
    from azure.ai.formrecognizer import DocumentAnalysisClient
    from azure.core.credentials import AzureKeyCredential

    AZURE_OCR_AVAILABLE = True
except ImportError:
    AZURE_OCR_AVAILABLE = False
    logger.warning("azure-ai-formrecognizer not available")


def _coerce_document_bytes(document: Union[bytes, Any]) -> bytes:
    if isinstance(document, (bytes, bytearray)):
        return bytes(document)
    if hasattr(document, "read"):
        data = document.read()
        if hasattr(document, "seek"):
            try:
                document.seek(0)
            except OSError:
                pass
        return data if isinstance(data, bytes) else bytes(data)
    raise TypeError("document must be bytes or a readable binary stream")


def _is_invalid_content_error(exc: BaseException) -> bool:
    """True when the service rejects the bytes as unsupported/corrupt raster (often WebP/GIF)."""
    normalized = str(exc).lower().replace(" ", "").replace("_", "")
    if "invalidcontent" in normalized:
        return True
    err = getattr(exc, "error", None)
    if err is not None:
        inner = getattr(err, "innererror", None) or {}
        if isinstance(inner, dict):
            icode = inner.get("code") or inner.get("Code")
            if icode and str(icode).lower() == "invalidcontent":
                return True
    return False


def _image_bytes_as_png_rgb(image_bytes: bytes) -> Optional[bytes]:
    """Raster formats unsupported by Document Intelligence are converted to PNG for OCR."""
    try:
        from PIL import Image
    except ImportError:
        return None
    try:
        with Image.open(io.BytesIO(image_bytes)) as im:
            # PDF / multi-page: leave to native Document Intelligence path
            if getattr(im, "format", None) == "PDF":
                return None
            if im.mode in ("RGBA", "LA"):
                background = Image.new("RGB", im.size, (255, 255, 255))
                background.paste(im, mask=im.split()[-1])
                im = background
            elif im.mode == "P" and "transparency" in im.info:
                rgba = im.convert("RGBA")
                background = Image.new("RGB", rgba.size, (255, 255, 255))
                background.paste(rgba, mask=rgba.split()[3])
                im = background
            elif im.mode != "RGB":
                im = im.convert("RGB")
            out = io.BytesIO()
            im.save(out, format="PNG")
            return out.getvalue()
    except Exception:
        return None


class OCRClient:
    def __init__(self, endpoint: str, key: Optional[str] = None):
        if not AZURE_OCR_AVAILABLE:
            raise ImportError("azure-ai-formrecognizer is required")

        self.endpoint = endpoint
        # Use key if provided, else Managed Identity
        if key:
            self.credential = AzureKeyCredential(key)
            logger.info("Initialized OCR Client with Key")
        else:
            self.credential = get_credential()
            logger.info("Initialized OCR Client with Managed Identity")

        self.client = DocumentAnalysisClient(
            endpoint=self.endpoint, credential=self.credential
        )

    def _analyze_bytes(self, data: bytes) -> str:
        poller = self.client.begin_analyze_document("prebuilt-read", document=data)
        result = poller.result()
        return " ".join(
            [line.content for page in result.pages for line in page.lines]
        )

    def extract_text(self, file_stream: Any) -> str:
        """
        Extract text from a file stream or bytes (PDF/Image) using 'prebuilt-read'.

        If the service returns InvalidContent (unsupported raster format, etc.),
        a single retry is done by transcoding images to PNG via Pillow when possible.
        """
        data = _coerce_document_bytes(file_stream)
        try:
            return self._analyze_bytes(data)
        except Exception as e:
            if not _is_invalid_content_error(e):
                logger.error("OCR Extraction failed: %s", e)
                raise
            png = _image_bytes_as_png_rgb(data)
            if not png:
                logger.warning(
                    "OCR skipped unsupported or unreadable content (InvalidContent): %s",
                    e,
                )
                raise
            try:
                return self._analyze_bytes(png)
            except Exception as e2:
                logger.error("OCR Extraction failed after PNG transcoding: %s", e2)
                raise e2 from e
