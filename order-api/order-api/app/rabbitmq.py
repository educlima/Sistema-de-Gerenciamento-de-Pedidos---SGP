import json
import logging

import pika
from tenacity import Retrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import settings

logger = logging.getLogger(__name__)


def _get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(
        settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD
    )
    parameters = pika.ConnectionParameters(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        credentials=credentials,
    )
    return pika.BlockingConnection(parameters)


def _publish_once(message: dict) -> None:
    connection = _get_connection()
    try:
        channel = connection.channel()
        channel.queue_declare(queue=settings.RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=settings.RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # mensagem persistente
            ),
        )
    finally:
        connection.close()


def _publish_with_retry(message: dict) -> None:
   
    retryer = Retrying(
        reraise=True,
        stop=stop_after_attempt(settings.MESSAGING_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=settings.MESSAGING_RETRY_MIN_WAIT_SECONDS,
            max=settings.MESSAGING_RETRY_MAX_WAIT_SECONDS,
        ),
        retry=retry_if_exception_type(
            (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError)
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    retryer(_publish_once, message)


def publish_order_created(order: dict) -> None:
    """Publica uma mensagem na fila do RabbitMQ informando a criação do pedido.

    Comportamento de resiliência:
    - Em caso de falha de conexão, tenta novamente automaticamente
      (MESSAGING_RETRY_ATTEMPTS tentativas, com backoff exponencial).
    - Se todas as tentativas falharem, o erro é registrado em log, mas
      NÃO é propagado: o cadastro do pedido no MongoDB já foi concluído
      e não deve ser perdido por uma instabilidade de mensageria.
    """
    if settings.DISABLE_RABBITMQ:
        logger.info("RabbitMQ desabilitado; mensagem não enviada: %s", order)
        return

    message = {
        "event": "order_created",
        "order_id": order["id"],
        "customer_name": order["customer_name"],
        "product_name": order["product_name"],
        "quantity": order["quantity"],
        "status": order["status"],
    }

    try:
        _publish_with_retry(message)
        logger.info("Mensagem publicada no RabbitMQ: %s", message)
    except Exception:
        logger.exception(
            "Falha ao publicar mensagem no RabbitMQ após %s tentativa(s). "
            "O pedido %s já foi salvo no MongoDB e não será perdido.",
            settings.MESSAGING_RETRY_ATTEMPTS,
            order.get("id"),
        )
