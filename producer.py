"""
Kafka producer: три способа отправки сообщений.

1) Fire-and-forget  — отправил и забыл
2) Synchronous      — ждём подтверждение брокера
3) Asynchronous     — не блокируем цикл, обрабатываем результат в callback

Перед запуском:
  kubectl port-forward -n kafka svc/kafka-controller-0-external 30093:9094

  python producer.py
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer
import json
import time
from typing import Any

BOOTSTRAP_SERVERS = ["localhost:30093"]
TOPIC = "order"


class JsonSerializer(Serializer):
    """Превращает Python-объект (dict/list) в JSON-байты."""

    def serialize(self, topic, data):
        if data is None:
            return None
        return json.dumps(data, ensure_ascii=False).encode("utf-8")


class StringSerializer(Serializer):
    """Превращает строковый key в bytes."""

    def serialize(self, topic, data):
        if data is None:
            return None
        return data.encode("utf-8")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=JsonSerializer(),
        key_serializer=StringSerializer(),
        # Для fire-and-forget часто ставят acks=0 или acks=1.
        # Для учёбы оставляем acks=all: надёжнее видно разницу режимов.
        acks="all",
        retries=3,
        linger_ms=10,
    )


def build_message(mode: str, i: int) -> dict[str, Any]:
    return {
        "mode": mode,
        "id": i,
        "text": f"{mode} сообщение #{i}",
        "ts": time.time(),
    }


# ---------------------------------------------------------------------------
# 1) Fire-and-forget: сделать и забыть
# ---------------------------------------------------------------------------
def send_fire_and_forget(producer: KafkaProducer, count: int = 3) -> None:
    """
    Отправляем сообщение и НЕ ждём результат.

    Плюсы:
      - максимальная скорость
      - простой код
    Минусы:
      - не знаем, дошло ли сообщение
      - ошибки легко пропустить
    """
    print("\n=== 1) Fire-and-forget ===")

    for i in range(count):
        message = build_message("fire-and-forget", i)

        # send() кладёт сообщение в буфер producer'а и сразу возвращает Future.
        # Мы Future игнорируем — это и есть "отправил и забыл".
        producer.send(
            TOPIC,
            key=f"ff-{i}",
            value=message,
        )
        print(f"queued (no wait): {message}")

    # Буфер ещё может быть не отправлен в сеть.
    # flush() здесь опционален: без него часть сообщений может потеряться
    # при быстром close(). Для демо оставляем, чтобы сообщения точно ушли.
    producer.flush()
    print("fire-and-forget: flush done")


# ---------------------------------------------------------------------------
# 2) Синхронная отправка
# ---------------------------------------------------------------------------
def send_sync(producer: KafkaProducer, count: int = 3) -> None:
    """
    После каждой send() ждём подтверждение брокера через future.get().

    Плюсы:
      - сразу видим успех/ошибку
      - проще отладка
    Минусы:
      - медленнее: ждём сеть + брокер на каждое сообщение
    """
    print("\n=== 2) Synchronous send ===")

    for i in range(count):
        message = build_message("sync", i)

        future = producer.send(
            TOPIC,
            key=f"sync-{i}",
            value=message,
        )

        try:
            # Блокируемся, пока брокер не подтвердит запись (или не будет ошибка).
            metadata = future.get(timeout=10)
            print(
                f"confirmed: topic={metadata.topic} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset} "
                f"value={message}"
            )
        except KafkaError as e:
            # Ошибка по конкретному сообщению — можно залогировать и продолжить
            # или прервать отправку, в зависимости от бизнес-логики.
            print(f"sync send failed for {message}: {e}")
            raise


# ---------------------------------------------------------------------------
# 3) Асинхронная отправка
# ---------------------------------------------------------------------------
def _on_send_success(metadata, message: dict[str, Any]) -> None:
    """Callback при успешной доставке."""
    print(
        f"async OK: topic={metadata.topic} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset} "
        f"value={message}"
    )


def _on_send_error(exc: Exception, message: dict[str, Any]) -> None:
    """Callback при ошибке доставки."""
    print(f"async FAIL: value={message} error={exc}")


def send_async(producer: KafkaProducer, count: int = 3) -> None:
    """
    Отправляем сообщения без блокировки цикла.
    Результат обрабатываем позже через callback на Future.

    Плюсы:
      - быстрее sync (не ждём каждое сообщение в цикле)
      - ошибки всё равно обрабатываем
    Минусы:
      - сложнее порядок логов/обработки
      - нужно помнить про flush()/close() в конце
    """
    print("\n=== 3) Asynchronous send ===")

    for i in range(count):
        message = build_message("async", i)

        future = producer.send(
            TOPIC,
            key=f"async-{i}",
            value=message,
        )

        # add_callback / add_errback вызываются в I/O-потоке producer'а,
        # когда брокер ответил. Цикл for при этом не блокируется.
        future.add_callback(_on_send_success, message)
        future.add_errback(_on_send_error, message)

        print(f"queued async: {message}")

    # Ждём, пока все async-отправки завершатся и сработают callback'и.
    producer.flush()
    print("async: flush done (callbacks should have finished)")


def main() -> None:
    producer = create_producer()

    try:
        send_fire_and_forget(producer)
        send_sync(producer)
        send_async(producer)
        print("\nall modes done")
    except KafkaError as e:
        print(f"Kafka error: {e}")
        raise
    finally:
        # close() сам делает flush и освобождает ресурсы.
        producer.close()


if __name__ == "__main__":
    main()
