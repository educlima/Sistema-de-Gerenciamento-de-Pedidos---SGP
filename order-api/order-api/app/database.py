from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    """Retorna (criando se necessário) o cliente assíncrono do MongoDB."""
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(settings.MONGO_URI)
    return _client


def get_collection():
    """Retorna a coleção utilizada para armazenar os pedidos."""
    client = get_client()
    db = client[settings.MONGO_DB_NAME]
    return db[settings.MONGO_COLLECTION]


def close_client() -> None:
    """Fecha a conexão com o MongoDB (usado no shutdown da aplicação)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
