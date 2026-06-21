import logging

from fastapi import FastAPI

from app.routes import router as orders_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Sistema de Gerenciamento de Pedidos - SGP",
    description=(
        "API para gerenciamento de pedidos, com persistência em MongoDB "
        "e publicação de eventos em RabbitMQ e Kafka."
    ),
    version="1.9.0",
)

app.include_router(orders_router)


@app.get("/health", tags=["Health"])
async def health_check() -> dict:
    """Endpoint simples de verificação de saúde da aplicação."""
    return {"status": "ok"}
