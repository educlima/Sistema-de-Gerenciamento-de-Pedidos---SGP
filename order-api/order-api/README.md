# Sistema de Gerenciamento de Pedidos — SGP

API REST para gerenciamento de pedidos, construída com **FastAPI**, com persistência em **MongoDB** e publicação de eventos em **RabbitMQ** e **Kafka**.

## Stack

- **FastAPI** — API REST com documentação automática (Swagger/OpenAPI)
- **MongoDB** — persistência dos pedidos
- **RabbitMQ** — fila de eventos `orders_created_queue`
- **Apache Kafka** — tópico de eventos `orders.created`
- **Docker Compose** — orquestração de toda a infraestrutura

## Requisitos atendidos

| Requisito                                                     | Implementação                     |
| ------------------------------------------------------------- | --------------------------------- |
| Cadastro de pedido (id, cliente, produto, quantidade, status) | `POST /orders`                    |
| Status inicial `PENDENTE`                                     | `app/models.py`                   |
| Persistência em MongoDB                                       | `app/database.py`                 |
| Publicação em fila RabbitMQ                                   | `app/rabbitmq.py`                 |
| Publicação em tópico Kafka                                    | `app/kafka_producer.py`           |
| Listagem de pedidos                                           | `GET /orders`                     |
| Testes automatizados                                          | `tests/test_orders.py` (8 testes) |
| Subida com 1 comando                                          | `docker-compose.yml`              |

## Como executar

Pré-requisito: [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e em execução.

```bash
git clone https://github.com/educlima)
cd order-api
docker compose up --build
```

Isso sobe os 6 serviços (API, MongoDB, RabbitMQ, Kafka, Zookeeper e o container auxiliar `kafka-init`, que cria o tópico Kafka e encerra). A API fica disponível quando aparecer:

```
Uvicorn running on http://0.0.0.0:8000
```

Para encerrar:

```bash
docker compose down
```

## Endpoints

### `POST /orders`

```json
{
  "customer_name": "João da Silva",
  "product_name": "Teclado Mecânico",
  "quantity": 2
}
```

**Resposta (`201`):**

```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "customer_name": "João da Silva",
  "product_name": "Teclado Mecânico",
  "quantity": 2,
  "status": "PENDENTE"
}
```

Ao cadastrar, a API gera um `id` único, persiste o pedido no MongoDB, publica na fila RabbitMQ e publica no tópico Kafka.

### `GET /orders`

Lista todos os pedidos cadastrados.

Documentação interativa completa em `http://localhost:8000/docs`.

## Validação manual

| O quê                 | Onde                                                                                                                        |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Cadastro e listagem   | Swagger em `http://localhost:8000/docs`                                                                                     |
| Mensagens no RabbitMQ | Painel em `http://localhost:15672` → Queues → `orders_created_queue` → _Get messages_                                       |
| Mensagens no Kafka    | `docker exec -it orders_kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic orders.created --from-beginning` |
| Dados no MongoDB      | `docker exec -it orders_mongodb mongosh orders_db --eval "db.orders.find().pretty()"`                                       |

## Resiliência da mensageria

Se RabbitMQ ou Kafka estiverem indisponíveis no momento da publicação, a API tenta novamente automaticamente (3 tentativas, backoff exponencial de 1s a 5s). Se todas as tentativas falharem, o erro é registrado em log, mas **o pedido permanece salvo no MongoDB e a API responde `201` normalmente** — nenhum pedido é perdido por instabilidade na mensageria.

## Testes automatizados

```bash
docker compose run --rm api pytest -v
```

Os testes usam `mongomock-motor` e mocks de RabbitMQ/Kafka, portanto não dependem da infraestrutura real — cobrem cadastro, listagem, validação de payload e resiliência da mensageria.

## Arquitetura

```
order-api/
├── app/
│   ├── main.py            # Inicialização da aplicação FastAPI
│   ├── routes.py          # Endpoints (POST /orders, GET /orders)
│   ├── models.py          # Modelos Pydantic
│   ├── database.py        # Conexão com MongoDB
│   ├── rabbitmq.py        # Publisher RabbitMQ
│   ├── kafka_producer.py  # Producer Kafka
│   └── config.py          # Configuração via variáveis de ambiente
├── infra/
│   ├── mongodb/init-mongo.js
│   ├── rabbitmq/definitions.json
│   └── kafka/create-topics.sh
├── tests/
├── Dockerfile
└── docker-compose.yml
```

## Variáveis de ambiente

Todas têm valores padrão (`app/config.py`, `.env.example`) já alinhados ao `docker-compose.yml`; não é necessário alterar nada para rodar localmente.

| Variável                   | Padrão                    |
| -------------------------- | ------------------------- |
| `MONGO_URI`                | `mongodb://mongodb:27017` |
| `MONGO_DB_NAME`            | `orders_db`               |
| `RABBITMQ_HOST`            | `rabbitmq`                |
| `RABBITMQ_QUEUE`           | `orders_created_queue`    |
| `KAFKA_BOOTSTRAP_SERVERS`  | `kafka:9092`              |
| `KAFKA_TOPIC`              | `orders.created`          |
| `MESSAGING_RETRY_ATTEMPTS` | `3`                       |
