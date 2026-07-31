"""
Kafka Avro producer: три способа отправки сообщений.

1) Fire-and-forget  — отправил и забыл
2) Synchronous      — ждём подтверждение брокера
3) Asynchronous     — обрабатываем результат в callback

Перед запуском (два терминала):

  kubectl apply -f schema-registry.yaml
  kubectl rollout status deployment/schema-registry -n kafka

  kubectl port-forward -n kafka svc/kafka-controller-0-external 30093:9094
  kubectl port-forward -n kafka svc/schema-registry 8081:8081

  pip install -r requirements.txt
  python producer.py
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, KafkaException, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, StringSerializer

BOOTSTRAP_SERVERS = "localhost:30093"
SCHEMA_REGISTRY_URL = "http://localhost:8081/apis/ccompat/v7"
TOPIC = "order"
SCHEMA_PATH = Path(__file__).with_name("order.avsc")


def load_schema() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def order_to_dict(order: dict[str, Any], ctx: SerializationContext) -> dict[str, Any]:
    """
    AvroSerializer вызывает to_dict перед кодированием.
    У нас value уже dict, совместимый со схемой — возвращаем как есть.
    """
    return order


def create_producer() -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client,
        load_schema(),
        order_to_dict,
    )

    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # key — обычная строка
            "key.serializer": StringSerializer("utf_8"),
            # value — Avro + Schema Registry
            "value.serializer": avro_serializer,
            "acks": "all",
            "retries": 3,
            "linger.ms": 10,
        }
    )


def build_message(mode: str, i: int) -> dict[str, Any]:
    # Поля должны совпадать с order.avsc
    return {
        "mode": mode,
        "id": i,
        "text": f"{mode} сообщение #{i}",
        "ts": time.time(),
    }


# ---------------------------------------------------------------------------
# 1) Fire-and-forget: сделать и забыть
# ---------------------------------------------------------------------------
def send_fire_and_forget(producer: SerializingProducer, count: int = 3) -> None:
    """
    produce() без ожидания результата.

    Плюсы: быстро и просто.
    Минусы: не знаем, дошло ли сообщение.
    """
    print("\n=== 1) Fire-and-forget ===")

    for i in range(count):
        message = build_message("fire-and-forget", i)

        # on_delivery не передаём — результат не обрабатываем
        producer.produce(
            topic=TOPIC,
            key=f"ff-{i}",
            value=message,
        )
        # poll(0) обслуживает внутренние очереди клиента, не блокирует
        producer.poll(0)
        print(f"queued (no wait): {message}")

    # Для демо flush(), иначе при быстром выходе часть сообщений
    # может остаться в буфере. В чистом fire-and-forget flush можно не делать.
    producer.flush()
    print("fire-and-forget: flush done")


# ---------------------------------------------------------------------------
# 2) Синхронная отправка
# ---------------------------------------------------------------------------
def send_sync(producer: SerializingProducer, count: int = 3) -> None:
    """
    После каждого produce() делаем flush() — ждём доставку.

    Плюсы: сразу видим успех/ошибку.
    Минусы: медленнее.
    """
    print("\n=== 2) Synchronous send ===")

    for i in range(count):
        message = build_message("sync", i)
        delivery: dict[str, Any] = {"error": None, "msg": None}

        def on_delivery(err, msg, result=delivery):
            result["error"] = err
            result["msg"] = msg

        producer.produce(
            topic=TOPIC,
            key=f"sync-{i}",
            value=message,
            on_delivery=on_delivery,
        )

        # flush() блокируется, пока сообщение не будет доставлено (или ошибка)
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            raise KafkaException(f"sync flush timeout, remaining={remaining}")

        if delivery["error"] is not None:
            raise KafkaException(delivery["error"])

        msg = delivery["msg"]
        print(
            f"confirmed: topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"value={message}"
        )


# ---------------------------------------------------------------------------
# 3) Асинхронная отправка
# ---------------------------------------------------------------------------
def _on_delivery(err, msg) -> None:
    """Общий callback для async-режима."""
    if err is not None:
        print(f"async FAIL: {err}")
        return

    print(
        f"async OK: topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()} "
        f"key={msg.key()}"
    )


def send_async(producer: SerializingProducer, count: int = 3) -> None:
    """
    produce() с on_delivery: цикл не ждёт каждое сообщение.
    Результаты приходят позже в callback.

    Плюсы: быстрее sync, ошибки всё равно ловим.
    Минусы: порядок логов может отличаться от порядка отправки.
    """
    print("\n=== 3) Asynchronous send ===")

    for i in range(count):
        message = build_message("async", i)

        producer.produce(
            topic=TOPIC,
            key=f"async-{i}",
            value=message,
            on_delivery=_on_delivery,
        )
        producer.poll(0)
        print(f"queued async: {message}")

    producer.flush()
    print("async: flush done (callbacks should have finished)")


def main() -> None:
    print(f"Kafka bootstrap: {BOOTSTRAP_SERVERS}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Topic: {TOPIC}")
    print(f"Schema: {SCHEMA_PATH.name}")

    producer = create_producer()

    try:
        send_fire_and_forget(producer)
        send_sync(producer)
        send_async(producer)
        print("\nall modes done")
    except (KafkaException, KafkaError) as e:
        print(f"Kafka error: {e}")
        raise
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
