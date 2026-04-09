"""Test: Verificar que el splitting funciona con helpers.json (3.5 MB)
y que se sube correctamente a Cosmos DB.
"""
import asyncio
import json
import sys

# Agregar el root del proyecto al path
sys.path.insert(0, "/home/dk_marlon_marin/procaps-framework-ai")

from src.infrastructure.cosmos import (
    CosmosDB,
    COSMOS_MAX_DOC_BYTES,
    get_cosmos_client,
    get_cosmos_database,
)
from config.settings import settings
from src.utils.app_logger import configure_logging, get_logger

configure_logging()
logger = get_logger("test_cosmos_split")


def test_split_only():
    """Fase 1: Verificar splitting local sin tocar Cosmos."""
    with open("./azpocdk/helpers.json", "r") as f:
        document = json.load(f)

    doc_size = CosmosDB._estimate_doc_size(document)
    print(f"\n{'='*60}")
    print(f"📊 Tamaño original del documento: {doc_size/1024:.1f} kB ({doc_size/1024/1024:.2f} MB)")
    print(f"📏 Límite de Cosmos:               {COSMOS_MAX_DOC_BYTES/1024:.1f} kB ({COSMOS_MAX_DOC_BYTES/1024/1024:.2f} MB)")
    print(f"🔴 Excede límite:                  {'SÍ' if doc_size > COSMOS_MAX_DOC_BYTES else 'NO'}")
    print(f"{'='*60}\n")

    if doc_size <= COSMOS_MAX_DOC_BYTES:
        print("✅ El documento cabe en un solo documento. No se necesita split.")
        return

    # Probar split con file_sizes (la clave más grande)
    storage_account = "azstapropdev"
    medallion = "bronze"
    splittable_key = "file_sizes"

    parts = CosmosDB._split_document(
        document, storage_account, medallion, splittable_key
    )

    print(f"📦 Documento dividido en {len(parts)} parte(s):\n")
    for i, part in enumerate(parts, 1):
        part_size = CosmosDB._estimate_doc_size(part)
        part_entries = len(part[storage_account][medallion][splittable_key])
        fits = "✅" if part_size <= COSMOS_MAX_DOC_BYTES else "❌ EXCEDE LÍMITE"
        print(f"  Parte {i}/{len(parts)}: id={part['id']}")
        print(f"    Tamaño:    {part_size/1024:.1f} kB ({part_size/1024/1024:.2f} MB) {fits}")
        print(f"    Entradas:  {part_entries} en '{splittable_key}'")
        print(f"    _part:     {part.get('_part')}")
        print(f"    _total:    {part.get('_total_parts')}")
        print()

    # Verificar que todas las partes caben
    all_fit = all(
        CosmosDB._estimate_doc_size(p) <= COSMOS_MAX_DOC_BYTES
        for p in parts
    )
    print(f"{'='*60}")
    print(f"{'✅ TODAS las partes caben en Cosmos' if all_fit else '❌ ALGUNAS partes exceden el límite'}")
    print(f"{'='*60}\n")

    # Verificar que no se perdieron datos
    original_keys = set(document[storage_account][medallion][splittable_key].keys())
    split_keys = set()
    for p in parts:
        split_keys.update(p[storage_account][medallion][splittable_key].keys())

    missing = original_keys - split_keys
    extra = split_keys - original_keys
    print(f"🔍 Integridad de datos:")
    print(f"   Claves originales: {len(original_keys)}")
    print(f"   Claves en partes:  {len(split_keys)}")
    print(f"   Perdidas:          {len(missing)}")
    print(f"   Extra:             {len(extra)}")
    print(f"   {'✅ Sin pérdida de datos' if not missing and not extra else '❌ HAY PÉRDIDA DE DATOS'}")

    return all_fit and not missing


async def test_upsert_to_cosmos():
    """Fase 2: Subir a Cosmos usando upsert_metadata con auto-split."""
    with open("./azpocdk/helpers.json", "r") as f:
        document = json.load(f)

    # Usar un doc_id de prueba para no sobreescribir datos reales
    document["id"] = "test-split-2026-04-10"

    cos_client = get_cosmos_client()
    database = get_cosmos_database(cos_client)

    cosmos = CosmosDB(
        db=database,
        container_name=settings.azure_cosmos_container,
        doc_id=document["id"],
    )

    print(f"\n{'='*60}")
    print("🚀 Subiendo documento a Cosmos con auto-split...")
    print(f"{'='*60}\n")

    result = await cosmos.upsert_metadata(
        storage_account="azstapropdev",
        medallion="bronze",
        document=document,
        splittable_key="file_sizes",
    )

    if isinstance(result, list):
        print(f"\n✅ Documento subido en {len(result)} partes a Cosmos DB.")
        for r in result:
            print(f"   → {r.get('id')}")
    else:
        print(f"\n✅ Documento subido como documento único: {result.get('id')}")


def main():
    # Fase 1: Test local
    print("\n🧪 FASE 1: Verificación local del splitting\n")
    split_ok = test_split_only()

    if not split_ok:
        print("\n❌ El splitting local falló, abortando upload.")
        return

    # Fase 2: Upload real a Cosmos
    print("\n\n🧪 FASE 2: Upload a Cosmos DB\n")
    response = input("¿Deseas subir el documento de prueba a Cosmos? (s/n): ").strip().lower()
    if response == "s":
        asyncio.run(test_upsert_to_cosmos())
    else:
        print("⏭️  Upload omitido.")


if __name__ == "__main__":
    main()
