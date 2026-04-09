import asyncio
import threading
from typing import Coroutine, Any, Dict
from src.infrastructure.cosmos import CosmosDB, get_cosmos_client, get_cosmos_database
from config.settings import settings

async def upload_report(
    container: str,
    medallion: str,
    document: Dict[str, Any],
    splittable_key: str = "",
    doc_id: str | None = None
):
    cos_client = get_cosmos_client()
    try:
        database = get_cosmos_database(cos_client)
        cosmos = CosmosDB(
            db=database,
            container_name=settings.azure_cosmos_container,
            doc_id=doc_id
        )
        await cosmos.upsert_metadata(
            storage_account=container,
            medallion=medallion,
            document=document,
            doc_id=cosmos._doc_id,
            splittable_key=splittable_key
        )
    finally:
        await cos_client.close()

def run_coro(coro: Coroutine) -> Any:
    """
    Ejecuta de manera segura una corrutina y devuelve su resultado.
    Si se detecta que ya existe un bucle de eventos corriendo (ej. en Databricks / Jupyter),
    se ejecuta la corrutina en un nuevo hilo para evitar RuntimeError.
    
    Args:
        coro: Corrutina asíncrona a ejecutar.
    Returns:
        El resultado de la ejecución de la corrutina.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Ejecutar en un nuevo thread para no bloquear el loop actual
        result_container = []
        exception_container = []

        def run_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                result = new_loop.run_until_complete(coro)
                result_container.append(result)
            except Exception as e:
                exception_container.append(e)
            finally:
                new_loop.close()

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()
        
        if exception_container:
            raise exception_container[0]
            
        return result_container[0] if result_container else None
    else:
        # Ejecución estándar local si no hay event loop corriendo
        return asyncio.run(coro)
