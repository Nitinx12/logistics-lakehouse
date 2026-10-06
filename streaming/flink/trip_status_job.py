# Consumes delivery events and emits trip status plus alerts (ARCHITECTURE.md §8.5).
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer, Producer  # noqa: E402

from src.utils.logger import get_logger  # noqa: E402
from streaming.flink.logic import (  # noqa: E402
    advance_watermark,
    delay_alert,
    is_late,
    late_alert,
    stuck_alerts,
    to_status,
)
from streaming.message import decode, dlq_headers, encode, parse_iso, validate_event  # noqa: E402
from streaming.topics import env  # noqa: E402

logger = get_logger(__name__)


def consumer_config(group: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": group,
        "auto.offset.reset": env("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
    }


def float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def emit(
    producer: Producer,
    topic: str,
    trip_id: str,
    record: dict[str, Any],
) -> None:
    producer.produce(topic, key=trip_id.encode("utf-8"), value=encode(record))


def run() -> int:
    events = env("KAFKA_TOPIC_DELIVERY_EVENTS", "lh.delivery_events.v1")
    status_topic = env("KAFKA_TOPIC_TRIP_STATUS", "lh.trip_status.v1")
    alerts_topic = env("KAFKA_TOPIC_DELIVERY_ALERTS", "lh.delivery_alerts.v1")
    dlq = env("KAFKA_TOPIC_DELIVERY_EVENTS_DLQ", "lh.delivery_events.v1.dlq")
    out_of_order_s = float_env("FLINK_WATERMARK_OUT_OF_ORDER_SECONDS", 30.0)
    allowed_lateness_s = float_env("FLINK_ALLOWED_LATENESS_SECONDS", 120.0)
    stuck_minutes = float_env("FLINK_STUCK_TRIP_MINUTES", 30.0)
    producer = Producer(
        {
            "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    consumer = Consumer(consumer_config(env("FLINK_GROUP", "lh-flink")))
    consumer.subscribe([events])
    last_seen: dict[str, datetime] = {}
    current_max: datetime | None = None
    swept_at = datetime.now(UTC)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("consumer error %s", message.error())
                continue
            try:
                event = decode(message.value())
            except ValueError as exc:
                producer.produce(
                    dlq,
                    key=message.key() or b"",
                    value=message.value() or b"",
                    headers=[
                        (key, value.encode("utf-8"))
                        for key, value in dlq_headers(
                            str(exc), events, message.offset()
                        ).items()
                    ],
                )
                consumer.commit(message=message)
                continue
            reason = validate_event(event)
            if reason:
                producer.produce(
                    dlq,
                    key=event.get("trip_id", "").encode("utf-8"),
                    value=encode(event),
                    headers=[
                        (key, value.encode("utf-8"))
                        for key, value in dlq_headers(
                            reason, events, message.offset()
                        ).items()
                    ],
                )
                consumer.commit(message=message)
                continue
            event_ts = parse_iso(event["event_ts"])
            current_max = advance_watermark(current_max, event_ts)
            watermark = current_max - timedelta(seconds=out_of_order_s)
            if is_late(event_ts, watermark, allowed_lateness_s):
                emit(producer, alerts_topic, event["trip_id"], late_alert(event))
            else:
                emit(producer, status_topic, event["trip_id"], to_status(event))
                alert = delay_alert(event)
                if alert is not None:
                    emit(producer, alerts_topic, event["trip_id"], alert)
                last_seen[event["trip_id"]] = event_ts
            producer.poll(0)
            consumer.commit(message=message)
            if (datetime.now(UTC) - swept_at).total_seconds() > 60:
                for stuck in stuck_alerts(last_seen, datetime.now(UTC), stuck_minutes):
                    emit(producer, alerts_topic, stuck["trip_id"], stuck)
                swept_at = datetime.now(UTC)
    except KeyboardInterrupt:
        return 0
    finally:
        producer.flush(10)
        consumer.close()


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001
        logger.error("stream job failed error=%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
