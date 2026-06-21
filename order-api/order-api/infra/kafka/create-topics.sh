# Este script cria o tópico "orders.created" no Kafka.
#
# Ele roda dentro de um container separado (kafka-init), definido no
# docker-compose.yml. Esse container só faz uma coisa: espera o Kafka
# ficar pronto, cria o tópico, e depois encerra.
#
# O loop abaixo fica tentando se conectar ao Kafka algumas vezes antes
# de desistir, como uma proteção extra caso ele demore um pouco mais
# para iniciar.
set -e

echo "Verificando disponibilidade do Kafka..."

attempt=0
max_attempts=15

until kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Kafka não respondeu após $max_attempts tentativas. Abortando."
    exit 1
  fi
  echo "Kafka ainda não disponível (tentativa $attempt/$max_attempts). Aguardando..."
  sleep 2
done

echo "Kafka disponível. Criando tópico 'orders.created' (se ainda não existir)..."

kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic orders.created \
  --partitions 1 \
  --replication-factor 1

echo "Tópico 'orders.created' pronto:"

kafka-topics --bootstrap-server kafka:9092 --describe --topic orders.created
