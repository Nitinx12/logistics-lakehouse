# Forwards delivery alerts to Alertmanager (ARCHITECTURE.md §8.1, §13.3).
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer  # noqa: E402

from src.utils.logger import get_logger  # noqa: E402
from streaming.flink.trip_status_job import consumer_config  # noqa: E402
from streaming.message import decode  # noqa: E402
from streaming.topics import env  # noqa: E402

logger = get_logger(__name__)


def alertmanager_url() -> str:
    import os

    return os.getenv(
        "ALERTMANAGER_URL", "http://alertmanager:9093/api/v2/alerts"
    ).strip()


def notify(url: str, alert: dict[str, Any]) -> None:
    body = json.dumps(
        [
            {
                "labels": {
                    "alertname": "DeliveryAlert",
                    "alert_type": alert.get("alert_type", "unknown"),
                    "trip_id": alert.get("trip_id", ""),
                    "severity": "warning",
                },
                "annotations": {"detail": json.dumps(alert.get("detail", {}))},
            }
        ]
    ).encode("utf-8")
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=5):
        pass


def run() -> int:
    topic = env("KAFKA_TOPIC_DELIVERY_ALERTS", "lh.delivery_alerts.v1")
    url = alertmanager_url()
    consumer = Consumer(consumer_config(env("CONSUMER_GROUP_ALERTS", "lh-alerts")))
    consumer.subscribe([topic])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("consumer error %s", message.error())
                continue
            try:
                alert = decode(message.value())
                logger.warning(
                    "delivery alert type=%s trip=%s",
                    alert.get("alert_type"),
                    alert.get("trip_id"),
                )
                try:
                    notify(url, alert)
                except OSError as exc:
                    logger.warning("alertmanager unreachable error=%s", exc)
            except ValueError as exc:
                logger.warning(
                    "skip undecodable offset=%s error=%s", message.offset(), exc
                )
            consumer.commit(message=message)
    except KeyboardInterrupt:
        return 0
    finally:
        consumer.close()


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001
        logger.error("alert consumer failed error=%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
