# Databricks notebook source
from src.infrastructure.connections import DocumentIntelligenceConnection, StorageAccount
from src.utils.app_logger import get_logger, configure_logging
from config.settings import settings
from src.core.models import AnalyzedDocument
import json, re, os
from typing import Optional

# ========== CARGA DE SETTINGS ==========
STEP_NAME = "s04_read_images"

IMAGE_EXTENSIONS = re.compile(r"\.(png|jpg|jpeg|tiff)$", re.IGNORECASE)

st_account = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key,
)

di_client = DocumentIntelligenceConnection(
    endpoint=settings.azure_document_intelligence_endpoint,
    key=settings.azure_document_intelligence_key,
)

with open("./azpocdk/grouped_paths.json", "r") as f:
    data = json.load(f)

# ========== LOGICA PRINCIPAL ==========


def load_image_files(
    container: str = "bronce",
    timeout: int = 60,
    sorted_by_size: bool = False,
    limit: Optional[int] = None,
) -> dict[str, bytes]:
    """Descarga imágenes desde el Storage Account.

    Args:
        container: Nombre del contenedor en Data Lake.
        timeout: Tiempo máximo de descarga por archivo (segundos).
        sorted_by_size: Si es True, ordena las imágenes por tamaño ascendente.
        limit: Cantidad máxima de imágenes a procesar.

    Returns:
        Diccionario ``{ruta_archivo: bytes_contenido}``.
    """
    image_paths: list[str] = data.get("images", [])

    if not image_paths:
        logger.warning("⚠️ No se encontraron imágenes en grouped_paths.json")
        return {}

    if sorted_by_size:
        logger.info("⌚ Ordenando imágenes por tamaño...")
        image_paths = sorted(
            image_paths,
            key=lambda x: st_account.get_file_size(container=container, file_path=x),
        )

    if limit:
        image_paths = image_paths[:limit]
        logger.info(f"🔢 Procesando las primeras {limit} imágenes")

    image_collection: dict[str, bytes] = {}
    for file_path in image_paths:
        if IMAGE_EXTENSIONS.search(file_path):
            logger.info(f"👁️ Descargando imagen: {file_path}")
            content = st_account.read_file(
                container=container, file_path=file_path, timeout=timeout
            )
            if content:
                image_collection[file_path] = content
                logger.info(
                    f"✅ Descarga de {file_path} completada ({len(content)} bytes)"
                )
            else:
                logger.warning(f"⚠️ Archivo vacío o error al leer: {file_path}")

    logger.info(f"📦 Total imágenes descargadas: {len(image_collection)}")
    return image_collection


def analyze_image(
    file_path: str,
    image_bytes: bytes,
    model: str = "prebuilt-read",
) -> Optional[AnalyzedDocument]:
    """Analiza una imagen con Azure Document Intelligence.

    Extrae texto, párrafos, tablas y metadata de la imagen
    (screenshot de documento, JSON, etc.).

    Args:
        file_path: Ruta original del archivo (para logging y metadata).
        image_bytes: Contenido de la imagen en bytes.
        model: Modelo de DI a utilizar. Defaults to ``"prebuilt-read"``.

    Returns:
        AnalyzedDocument con los resultados, o None si falla el análisis.
    """
    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else "unknown"
    logger.info(f"🔍 Analizando imagen: {file_path} (ext={ext}, model={model})")

    try:
        result = di_client.analyze_document_from_stream(
            document=image_bytes,
            file_type=ext,
            model=model,
        )
        logger.info(
            f"✅ Análisis completado: {file_path} "
            f"| páginas={result.pages} "
            f"| párrafos={len(result.paragraphs)} "
            f"| tablas={len(result.tables)}"
        )
        return result
    except Exception as e:
        logger.error(f"❌ Error al analizar {file_path}: {e}")
        return None


def analyze_all_images(
    image_collection: dict[str, bytes],
    model: str = "prebuilt-read",
) -> dict[str, AnalyzedDocument]:
    """Analiza todas las imágenes descargadas.

    Args:
        image_collection: Diccionario ``{ruta: bytes}`` de ``load_image_files``.
        model: Modelo de DI a utilizar.

    Returns:
        Diccionario ``{ruta: AnalyzedDocument}`` con resultados exitosos.
    """
    results: dict[str, AnalyzedDocument] = {}
    total = len(image_collection)

    for idx, (file_path, image_bytes) in enumerate(image_collection.items(), start=1):
        logger.info(f"📄 [{idx}/{total}] Procesando: {file_path}")
        analyzed = analyze_image(file_path, image_bytes, model=model)
        if analyzed:
            results[file_path] = analyzed

    logger.info(
        f"🏁 Análisis finalizado: {len(results)}/{total} imágenes procesadas correctamente"
    )
    return results


def export_results(
    results: dict[str, AnalyzedDocument],
    output_path: str = "./azpocdk",
    output_file: str = "image_analysis_results.json",
) -> None:
    """Exporta los resultados del análisis de imágenes a un archivo JSON.

    Args:
        results: Diccionario ``{ruta: AnalyzedDocument}``.
        output_path: Directorio de salida.
        output_file: Nombre del archivo de salida.
    """
    os.makedirs(output_path, exist_ok=True)
    full_path = os.path.join(output_path, output_file)

    serialized = {}
    for file_path, doc in results.items():
        serialized[file_path] = {
            "content": doc.content,
            "pages": doc.pages,
            "file_type": doc.file_type,
            "paragraphs": doc.paragraphs,
            "tables": doc.tables,
            "metadata": doc.metadata,
        }

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(serialized, f, indent=2, ensure_ascii=False)

    logger.info(f"💾 Resultados exportados a: {full_path}")


if __name__ == "__main__":
    configure_logging()
    logger = get_logger(STEP_NAME)

    logger.info(f"--- Iniciando paso: {STEP_NAME} ---")

    # 1. Descargar imágenes desde Storage
    image_collection = load_image_files(sorted_by_size=True, limit=5)

    # 2. Analizar cada imagen con Document Intelligence
    if image_collection:
        results = analyze_all_images(image_collection)

        # 3. Mostrar resumen de resultados
        for path, doc in results.items():
            logger.info(
                f"\n--- Resultado: {path} ---\n"
                f"  Contenido (primeros 200 chars): {doc.content[:200]}...\n"
                f"  Tablas encontradas: {len(doc.tables)}\n"
                f"  Metadata: {doc.metadata}"
            )

        # 4. Exportar resultados a JSON
        export_results(results)
    else:
        logger.warning("⚠️ No hay imágenes para analizar.")

    logger.info(f"--- Paso {STEP_NAME} finalizado ---")