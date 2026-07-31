# Kafka Python Producer (Minikube)

Учебный пример Kafka producer на Python с тремя режимами отправки:

1. **Fire-and-forget** — отправил и забыл
2. **Synchronous** — ждём подтверждение брокера
3. **Asynchronous** — callback на успех/ошибку

Также в репозитории есть `kafka-values.yaml` для развёртывания Kafka в Minikube через Helm (Bitnami, KRaft, `EXTERNAL://localhost:30093`).

## Требования

- Python 3.9+
- Minikube + Helm + kubectl
- Доступ к брокеру на `localhost:30093`

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Kafka в Minikube

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

kubectl create namespace kafka

helm install kafka bitnami/kafka \
  --version 32.4.3 \
  --namespace kafka \
  -f kafka-values.yaml
```

После старта:

```bash
kubectl rollout status statefulset/kafka-controller -n kafka

kubectl port-forward -n kafka svc/kafka-controller-0-external 30093:9094
```

В `producer.py` по умолчанию:

```python
BOOTSTRAP_SERVERS = ["localhost:30093"]
TOPIC = "order"
```

## Запуск producer

```bash
python producer.py
```

## Полезные команды

Список топиков:

```bash
kubectl run kafka-client \
  --restart=Never \
  --image=docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 \
  -n kafka \
  --command -- sleep infinity

kubectl exec -it kafka-client -n kafka -- \
  kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Чтение сообщений:

```bash
kubectl exec -it kafka-client -n kafka -- \
  kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic order \
    --partition 0 \
    --offset earliest
```
