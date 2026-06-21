import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
from tenacity import Retrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import settings

logger = logging.getLogger(__name__)

_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


def _publish_once(event: dict) -> None:
    """Envia o evento; em caso de falha, descarta o producer interno para
    forçar uma reconexão limpa na próxima tentativa."""
    global _producer
    try:
        producer = _get_producer()
        producer.send(settings.KAFKA_TOPIC, value=event)
        producer.flush(timeout=10)
    except (KafkaTimeoutError, NoBrokersAvailable, KafkaError):
        if _producer is not None:
            try:
                _producer.close()
            except Exception:
                pass
            _producer = None
        raise


def _publish_with_retry(event: dict) -> None:
    """Tenta publicar o evento, com retry automático (backoff exponencial)
    em caso de falha de conexão com o Kafka. Útil em redes instáveis.

    A política de retry é construída a cada chamada para respeitar os
    valores atuais de `settings`.
    """
    retryer = Retrying(
        reraise=True,
        stop=stop_after_attempt(settings.MESSAGING_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=settings.MESSAGING_RETRY_MIN_WAIT_SECONDS,
            max=settings.MESSAGING_RETRY_MAX_WAIT_SECONDS,
        ),
        retry=retry_if_exception_type((KafkaTimeoutError, NoBrokersAvailable, KafkaError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    retryer(_publish_once, event)


def publish_order_created(order: dict) -> None:
    """Publica um evento no tópico Kafka registrando a criação do pedido.

    Comportamento de resiliência:
    - Em caso de falha de conexão, tenta novamente automaticamente
      (MESSAGING_RETRY_ATTEMPTS tentativas, com backoff exponencial).
    - Se todas as tentativas falharem, o erro é registrado em log, mas
      NÃO é propagado: o cadastro do pedido no MongoDB já foi concluído
      e não deve ser perdido por uma instabilidade de mensageria.
    """
    if settings.DISABLE_KAFKA:
        logger.info("Kafka desabilitado; evento não enviado: %s", order)
        return

    event = {
        "event": "order_created",
        "order_id": order["id"],
        "customer_name": order["customer_name"],
        "product_name": order["product_name"],
        "quantity": order["quantity"],
        "status": order["status"],
    }

    try:
        _publish_with_retry(event)
        logger.info("Evento publicado no Kafka: %s", event)
    except Exception:
        logger.exception(
            "Falha ao publicar evento no Kafka após %s tentativa(s). "
            "O pedido %s já foi salvo no MongoDB e não será perdido.",
            settings.MESSAGING_RETRY_ATTEMPTS,
            order.get("id"),
        )
