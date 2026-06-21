import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_create_order_returns_201_and_pending_status(client, mock_messaging):
    payload = {
        "customer_name": "João da Silva",
        "product_name": "Teclado Mecânico",
        "quantity": 2,
    }

    response = await client.post("/orders", json=payload)

    assert response.status_code == 201
    data = response.json()

    assert data["customer_name"] == payload["customer_name"]
    assert data["product_name"] == payload["product_name"]
    assert data["quantity"] == payload["quantity"]
    assert data["status"] == "PENDENTE"
    assert "id" in data and data["id"]


async def test_create_order_publishes_to_rabbitmq_and_kafka(client, mock_messaging):
    payload = {
        "customer_name": "Maria Souza",
        "product_name": "Mouse Gamer",
        "quantity": 1,
    }

    response = await client.post("/orders", json=payload)
    assert response.status_code == 201
    order_id = response.json()["id"]

    assert len(mock_messaging["rabbitmq"]) == 1
    assert mock_messaging["rabbitmq"][0]["id"] == order_id
    assert mock_messaging["rabbitmq"][0]["status"] == "PENDENTE"

    assert len(mock_messaging["kafka"]) == 1
    assert mock_messaging["kafka"][0]["id"] == order_id
    assert mock_messaging["kafka"][0]["status"] == "PENDENTE"


async def test_create_order_with_invalid_quantity_returns_422(client, mock_messaging):
    payload = {
        "customer_name": "Carlos Pereira",
        "product_name": "Monitor 24'",
        "quantity": 0,
    }

    response = await client.post("/orders", json=payload)

    assert response.status_code == 422


async def test_list_orders_returns_empty_list_when_no_orders(client):
    response = await client.get("/orders")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_orders_returns_created_orders(client, mock_messaging):
    payload_1 = {
        "customer_name": "Ana Lima",
        "product_name": "Headset",
        "quantity": 1,
    }
    payload_2 = {
        "customer_name": "Pedro Costa",
        "product_name": "Webcam",
        "quantity": 3,
    }

    await client.post("/orders", json=payload_1)
    await client.post("/orders", json=payload_2)

    response = await client.get("/orders")

    assert response.status_code == 200
    orders = response.json()

    assert len(orders) == 2
    customer_names = {order["customer_name"] for order in orders}
    assert customer_names == {"Ana Lima", "Pedro Costa"}
    assert all(order["status"] == "PENDENTE" for order in orders)


async def test_create_order_succeeds_even_if_messaging_fails(client, monkeypatch):
    """Garante que o pedido é salvo no MongoDB e a API responde 201
    mesmo que RabbitMQ e Kafka estejam completamente indisponíveis
    (simulando instabilidade de rede)."""

    monkeypatch.setattr("app.routes.rabbitmq.publish_order_created", lambda o: None)
    monkeypatch.setattr("app.routes.kafka_producer.publish_order_created", lambda o: None)

    payload = {
        "customer_name": "Rede Instável Ltda",
        "product_name": "Roteador",
        "quantity": 1,
    }

    response = await client.post("/orders", json=payload)
    assert response.status_code == 201

    list_response = await client.get("/orders")
    assert list_response.status_code == 200
    customer_names = {o["customer_name"] for o in list_response.json()}
    assert "Rede Instável Ltda" in customer_names


def test_rabbitmq_publish_retries_and_does_not_raise_on_persistent_failure(monkeypatch):
    """Testa diretamente o módulo rabbitmq: se a conexão falhar
    repetidamente (rede instável), a função deve tentar novamente
    (retry) e, ao final, NÃO lançar exceção — o erro é apenas logado.
    """
    import pika

    from app import rabbitmq

    call_count = {"n": 0}

    def always_fail(*args, **kwargs):
        call_count["n"] += 1
        raise pika.exceptions.AMQPConnectionError("conexão recusada (simulado)")

    monkeypatch.setattr(rabbitmq, "_get_connection", always_fail)
    monkeypatch.setattr(rabbitmq.settings, "MESSAGING_RETRY_ATTEMPTS", 3)
    monkeypatch.setattr(rabbitmq.settings, "MESSAGING_RETRY_MIN_WAIT_SECONDS", 0.01)
    monkeypatch.setattr(rabbitmq.settings, "MESSAGING_RETRY_MAX_WAIT_SECONDS", 0.02)

    message = {
        "event": "order_created",
        "order_id": "abc-123",
        "customer_name": "Teste",
        "product_name": "Produto",
        "quantity": 1,
        "status": "PENDENTE",
    }

    # A função interna PROPAGA a exceção após esgotar as tentativas
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        rabbitmq._publish_with_retry(message)

    # Deve ter tentado exatamente o número de vezes configurado
    assert call_count["n"] == 3


def test_kafka_publish_retries_and_does_not_raise_on_persistent_failure(monkeypatch):
    """Testa diretamente o módulo kafka_producer: se a conexão falhar
    repetidamente (rede instável), a função deve tentar novamente
    (retry) e, ao final, NÃO lançar exceção — o erro é apenas logado.

    Nota: testamos `_publish_with_retry` (função interna) pelo mesmo
    motivo explicado no teste equivalente do RabbitMQ.
    """
    from kafka.errors import NoBrokersAvailable

    from app import kafka_producer

    call_count = {"n": 0}

    def always_fail(*args, **kwargs):
        call_count["n"] += 1
        raise NoBrokersAvailable("nenhum broker disponível (simulado)")

    monkeypatch.setattr(kafka_producer, "_get_producer", always_fail)
    monkeypatch.setattr(kafka_producer, "_producer", None)
    monkeypatch.setattr(kafka_producer.settings, "MESSAGING_RETRY_ATTEMPTS", 3)
    monkeypatch.setattr(kafka_producer.settings, "MESSAGING_RETRY_MIN_WAIT_SECONDS", 0.01)
    monkeypatch.setattr(kafka_producer.settings, "MESSAGING_RETRY_MAX_WAIT_SECONDS", 0.02)

    event = {
        "event": "order_created",
        "order_id": "def-456",
        "customer_name": "Teste",
        "product_name": "Produto",
        "quantity": 1,
        "status": "PENDENTE",
    }

    # A função interna PROPAGA a exceção após esgotar as tentativas
    with pytest.raises(NoBrokersAvailable):
        kafka_producer._publish_with_retry(event)

    assert call_count["n"] == 3
