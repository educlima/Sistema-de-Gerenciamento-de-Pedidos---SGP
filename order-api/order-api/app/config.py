import os


class Settings:
    """Configurações centrais da aplicação, lidas de variáveis de ambiente.

    Os valores padrão correspondem aos nomes de serviço definidos no
    docker-compose.yml, permitindo execução local sem configuração extra.
    """

    # MongoDB
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "orders_db")
    MONGO_COLLECTION: str = os.getenv("MONGO_COLLECTION", "orders")

    # RabbitMQ
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "rabbitmq")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_QUEUE: str = os.getenv("RABBITMQ_QUEUE", "orders_created_queue")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "orders.created")

    # Flags úteis para testes (permitem desligar integrações externas)
    DISABLE_RABBITMQ: bool = os.getenv("DISABLE_RABBITMQ", "false").lower() == "true"
    DISABLE_KAFKA: bool = os.getenv("DISABLE_KAFKA", "false").lower() == "true"

    # Retry de mensageria (resiliência a instabilidade de rede/broker)
    MESSAGING_RETRY_ATTEMPTS: int = int(os.getenv("MESSAGING_RETRY_ATTEMPTS", "3"))
    MESSAGING_RETRY_MIN_WAIT_SECONDS: float = float(
        os.getenv("MESSAGING_RETRY_MIN_WAIT_SECONDS", "1")
    )
    MESSAGING_RETRY_MAX_WAIT_SECONDS: float = float(
        os.getenv("MESSAGING_RETRY_MAX_WAIT_SECONDS", "5")
    )


settings = Settings()
