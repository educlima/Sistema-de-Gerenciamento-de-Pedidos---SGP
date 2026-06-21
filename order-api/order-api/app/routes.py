import logging

from fastapi import APIRouter, status

from app.database import get_collection
from app.models import Order, OrderCreate, OrderStatus
from app import kafka_producer, rabbitmq

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["Pedidos"])


@router.post("", response_model=Order, status_code=status.HTTP_201_CREATED)
async def create_order(payload: OrderCreate) -> Order:
    """Cadastra um novo pedido.

    Fluxo:
    1. Gera um identificador único e define o status inicial como PENDENTE;
    2. Persiste o pedido no MongoDB;
    3. Publica uma mensagem na fila RabbitMQ;
    4. Publica um evento no tópico Kafka.
    """
    order = Order(
        customer_name=payload.customer_name,
        product_name=payload.product_name,
        quantity=payload.quantity,
        status=OrderStatus.PENDENTE,
    )

    order_dict = order.model_dump()

    collection = get_collection()
    await collection.insert_one(dict(order_dict))

    rabbitmq.publish_order_created(order_dict)
    kafka_producer.publish_order_created(order_dict)

    return order


@router.get("", response_model=list[Order])
async def list_orders() -> list[Order]:
    """Lista todos os pedidos cadastrados no MongoDB."""
    collection = get_collection()
    cursor = collection.find({})
    orders = []
    async for document in cursor:
        document.pop("_id", None)
        orders.append(Order(**document))
    return orders
