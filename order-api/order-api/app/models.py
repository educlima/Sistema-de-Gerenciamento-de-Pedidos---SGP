import uuid
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class OrderStatus(str, Enum):
    PENDENTE = "PENDENTE"
    PROCESSANDO = "PROCESSANDO"
    CONCLUIDO = "CONCLUIDO"
    CANCELADO = "CANCELADO"


class OrderCreate(BaseModel):
    """Payload recebido para criação de um pedido."""

    customer_name: str = Field(..., min_length=1, description="Nome do cliente")
    product_name: str = Field(..., min_length=1, description="Nome do produto")
    quantity: int = Field(..., gt=0, description="Quantidade do produto")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "customer_name": "João da Silva",
                "product_name": "Teclado Mecânico",
                "quantity": 2,
            }
        }
    )


class Order(BaseModel):
    """Representação completa de um pedido, conforme persistido no MongoDB."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_name: str
    product_name: str
    quantity: int
    status: OrderStatus = OrderStatus.PENDENTE

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "customer_name": "João da Silva",
                "product_name": "Teclado Mecânico",
                "quantity": 2,
                "status": "PENDENTE",
            }
        }
    )
