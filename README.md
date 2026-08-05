# Kafka producer (Minikube + Avro)

Заметки по локальному producer'у на Python. Кластер кручу в Minikube, сообщения — в Avro через Schema Registry.

В `producer.py` три режима отправки:

1. fire-and-forget
2. sync (`flush` / ждём результат)
3. async (callback)

## Файлы

- `producer.py` — сам producer
- `order.avsc` — схема Avro
- `kafka-values.yaml` — Helm values для Bitnami Kafka
- `schema-registry.yaml` — Apicurio Registry
- `requirements.txt`

## Что нужно

- Python 3.9+
- Minikube, Helm, kubectl
- Kafka на `localhost:30093`
- Schema Registry на `localhost:8081`

## Поднять Kafka

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
kubectl create namespace kafka

export CLUSTER_ID=$(kubectl get secret kafka-kraft -n kafka -o jsonpath='{.data.cluster-id}' | base64 -d 2>/dev/null)

helm upgrade --install kafka bitnami/kafka \
  --version 32.4.3 \
  --namespace kafka \
  -f kafka-values.yaml \
  ${CLUSTER_ID:+--set-string clusterId=$CLUSTER_ID}
```

В `kafka-values.yaml` для одного брокера выставлен `offsets.topic.replication.factor: 1`.
Иначе не создаётся `__consumer_offsets`, и consumer groups / `subscribe()` не работают.

## Schema Registry

Обычный Confluent Schema Registry у меня на этой связке зависал на leader election.
Поставил Apicurio (in-memory) с Confluent-compatible API:

```bash
kubectl apply -f schema-registry.yaml
kubectl rollout status deployment/schema-registry -n kafka
```

URL в коде:

```python
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
```

## Port-forward

```bash
kubectl port-forward -n kafka svc/kafka-controller-0-external 30093:9094
kubectl port-forward -n kafka svc/schema-registry 8081:8081
```

Проверка registry:

```bash
curl -s http://localhost:8081/apis/ccompat/v7/subjects
```

## Запуск producer

```bash
pip install -r requirements.txt
python producer.py
```

```python
BOOTSTRAP_SERVERS = "localhost:30093"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
TOPIC = "order"
```

## Заметки

- сериализация через `AvroSerializer` / `StringSerializer` из `confluent-kafka`
- схема лежит в `order.avsc`, версии хранит Registry
- после первой отправки появляется subject `order-value`:

```bash
curl -s http://localhost:8081/apis/ccompat/v7/subjects
curl -s http://localhost:8081/apis/ccompat/v7/subjects/order-value/versions
```
