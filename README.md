> **Архив.** Актуальный код переехал в монорепо:
> https://github.com/AvelNight/kafka-minikube-lab

# Kafka Python Avro Producer (Minikube)

Учебный пример Kafka producer на Python + **Avro** + **Schema Registry** с тремя режимами отправки:

1. **Fire-and-forget** — отправил и забыл
2. **Synchronous** — ждём подтверждение брокера
3. **Asynchronous** — callback на успех/ошибку

## Состав репозитория

| Файл | Назначение |
|------|------------|
| `producer.py` | Avro producer, 3 режима отправки |
| `order.avsc` | Avro-схема сообщения |
| `kafka-values.yaml` | Helm values для Kafka в Minikube |
| `schema-registry.yaml` | Deployment Schema Registry |
| `requirements.txt` | Python-зависимости |

## Требования

- Python 3.9+
- Minikube + Helm + kubectl
- Kafka на `localhost:30093`
- Schema Registry на `localhost:8081`

## 1. Kafka (если ещё не установлена)

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
kubectl create namespace kafka

export CLUSTER_ID=$(kubectl get secret kafka-kraft -n kafka -o jsonpath='{.data.cluster-id}' | base64 -d 2>/dev/null)

# первая установка
helm upgrade --install kafka bitnami/kafka \
  --version 32.4.3 \
  --namespace kafka \
  -f kafka-values.yaml \
  ${CLUSTER_ID:+--set-string clusterId=$CLUSTER_ID}
```

## 2. Schema Registry (Apicurio, Confluent-compatible API)

Confluent Schema Registry в этой связке с Kafka 4 / Minikube зависал на leader election
(`Joining schema registry with Kafka-based coordination`). Для учёбы используем
**Apicurio Registry (in-memory)** с API, совместимым с Confluent SerDe:

```bash
kubectl apply -f schema-registry.yaml
kubectl rollout status deployment/schema-registry -n kafka
```

В `producer.py`:

```python
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
```

## 3. Port-forward (два терминала)

```bash
kubectl port-forward -n kafka svc/kafka-controller-0-external 30093:9094
```

```bash
kubectl port-forward -n kafka svc/schema-registry 8081:8081
```

Проверка Registry:

```bash
curl -s http://localhost:8081/subjects
```

## 4. Python

```bash
pip install -r requirements.txt
python producer.py
```

По умолчанию:

```python
BOOTSTRAP_SERVERS = "localhost:30093"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "order"
```

## Что изменилось по сравнению с JSON

- Вместо своих `Serializer`-классов используются `AvroSerializer` и `StringSerializer` из `confluent-kafka`
- Формат value задаётся схемой `order.avsc`
- Schema Registry хранит версию схемы; в сообщение пишется schema id

## Полезные команды

Список схем:

```bash
curl -s http://localhost:8081/subjects
```

Версии схемы топика (после первой отправки):

```bash
curl -s http://localhost:8081/subjects/order-value/versions
```
