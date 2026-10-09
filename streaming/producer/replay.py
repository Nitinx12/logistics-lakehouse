# Replays Mongo delivery_events into Kafka in event-time order (ARCHITECTURE.md §8.1).
import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Producer

from src.utils.connection import close_connection, get_mongo_database
from src.utils.logger import get_logger
from streaming.message import (
    build_event,
    dlq_headers,
    encode,
    parse_iso,
    utc_now_iso,
    validate_event,
)
from streaming.topics import env

logger = get_logger(__name__)


def producer_config() -> dict[str, Any]:
    return {
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "acks": env("PRODUCER_ACKS", "all"),
        "enable.idempotence": env("PRODUCER_ENABLE_IDEMPOTENCE", "true") == "true",
        "compression.type": env("PRODUCER_COMPRESSION", "zstd"),
        "linger.ms": int(env("PRODUCER_LINGER_MS", "30")),
    }


def replay_filter() -> dict[str, Any]:
    start = os.getenv("REPLAY_START_DATE", "").strip()
    end = os.getenv("REPLAY_END_DATE", "").strip()
    bounds: dict[str, Any] = {}
    if start:
        bounds["$gte"] = start
    if end:
        bounds["$lte"] = end
    if bounds:
        return {"actual_datetime": bounds}
    return {}


def speed_factor() -> float:
    try:
        return max(float(os.getenv("REPLAY_SPEED_FACTOR", "100")), 1.0)
    except ValueError:
        return 100.0


def max_gap_seconds() -> float:
    try:
        return max(float(os.getenv("REPLAY_MAX_GAP_SECONDS", "5")), 0.0)
    except ValueError:
        return 5.0


def header_items(headers: dict[str, str]) -> list[tuple[str, bytes]]:
    return [(key, value.encode("utf-8")) for key, value in headers.items()]


def replay(rate_limit: int = 0) -> int:
    topic = env("KAFKA_TOPIC_DELIVERY_EVENTS", "lh.delivery_events.v1")
    dlq = env("KAFKA_TOPIC_DELIVERY_EVENTS_DLQ", "lh.delivery_events.v1.dlq")
    producer = Producer(producer_config())
    database = get_mongo_database()
    try:
        cursor = (
            database["delivery_events"].find(replay_filter()).sort("actual_datetime", 1)
        )
        sent = invalid = 0
        previous_ts = None
        factor = speed_factor()
        for doc in cursor:
            message = build_event(doc)
            reason = validate_event(message)
            if reason:
                producer.produce(
                    dlq,
                    key=str(doc.get("trip_id", "")).encode("utf-8"),
                    value=encode({"doc": str(doc.get("_id", ""))}),
                    headers=header_items(dlq_headers(reason, topic, -1)),
                )
                invalid += 1
                continue
            event_ts = parse_iso(message["event_ts"])
            if previous_ts is not None:
                gap = (event_ts - previous_ts).total_seconds() / factor
                if gap > 0:
                    time.sleep(min(gap, max_gap_seconds()))
            previous_ts = event_ts
            producer.produce(
                topic,
                key=message["trip_id"].encode("utf-8"),
                value=encode(message),
                headers=[("schema_version", b"1"), ("source", b"replay-producer")],
                on_delivery=(
                    lambda error, _msg, eid=message["event_id"]: (
                        logger.warning("delivery failed event=%s error=%s", eid, error)
                        if error
                        else None
                    )
                ),
            )
            sent += 1
            if rate_limit and sent >= rate_limit:
                break
            if sent % 25 == 0:
                producer.poll(0)
                logger.info("replay progress sent=%s invalid=%s", sent, invalid)
        producer.flush(30)
        logger.info(
            "replay done sent=%s invalid=%s at=%s", sent, invalid, utc_now_iso()
        )
        return 0
    finally:
        close_connection(database.client)


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay delivery events to Kafka")
    parser.add_argument("--limit", type=int, default=0, help="Max events, 0 means all")
    try:
        return replay(parser.parse_args().limit)
    except Exception as exc:  # noqa: BLE001
        logger.error("replay failed error=%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
