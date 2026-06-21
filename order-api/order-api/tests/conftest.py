import pytest
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

from app import database
from app.main import app


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    """Substitui o MongoDB real por um cliente em memória (mongomock)."""
    mock_client = AsyncMongoMockClient()

    def fake_get_collection():
        db = mock_client["orders_db_test"]
        return db["orders"]

    monkeypatch.setattr(database, "get_collection", fake_get_collection)
    monkeypatch.setattr("app.routes.get_collection", fake_get_collection)
    return mock_client


@pytest.fixture(autouse=True)
def mock_messaging(monkeypatch):
    """Evita chamadas reais ao RabbitMQ e ao Kafka durante os testes,
    registrando as mensagens publicadas para que possam ser inspecionadas.
    """
    published = {"rabbitmq": [], "kafka": []}

    def fake_rabbitmq_publish(order: dict) -> None:
        published["rabbitmq"].append(order)

    def fake_kafka_publish(order: dict) -> None:
        published["kafka"].append(order)

    monkeypatch.setattr("app.routes.rabbitmq.publish_order_created", fake_rabbitmq_publish)
    monkeypatch.setattr("app.routes.kafka_producer.publish_order_created", fake_kafka_publish)

    return published


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
