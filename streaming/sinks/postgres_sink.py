# Upserts raw delivery events into bronze (ARCHITECTURE.md §8.6).
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from confluent_kafka import Consumer, Message  # noqa: E402

from src.utils.connection import close_connection, get_postgres_connection  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402
from streaming.flink.trip_status_job import consumer_config  # noqa: E402
from streaming.message import decode, validate_event  # noqa: E402
from streaming.topics import env  # noqa: E402

logger = get_logger(__name__)

UPSERT_SQL = """
INSERT INTO bronze.delivery_events_stream (
    event_id, trip_id, event_type, event_ts, payload, batch_id
) VALUES (%s, %s, %s, %s, %s::JSONB, %s)
ON CONFLICT (event_id) DO UPDATE SET
    trip_id = EXCLUDED.trip_id,
    event_type = EXCLUDED.event_type,
    event_ts = EXCLUDED.event_ts,
    payload = EXCLUDED.payload,
    batch_id = EXCLUDED.batch_id
    WHERE bronze.delivery_events_stream.event_ts <= EXCLUDED.event_ts
"""


def batch_size() -> int:
    try:
        return max(int(os.getenv("SINK_BATCH_SIZE", "500")), 1)
    except ValueError:
        return 500


def ensure_table() -> None:
    connection = get_postgres_connection()
    try:
        with connection.cursor() as cur:
            cur.execute("SELECT to_regclass('bronze.delivery_events_stream')")
            if cur.fetchone()[0] is None:
                raise RuntimeError("bronze.delivery_events_stream is missing")
    finally:
        close_connection(connection)


def build_rows(events: list[dict[str, Any]], run_id: str) -> list[tuple]:
    import json

    return [
        (
            event["event_id"],
            event["trip_id"],
            event["event_type"],
            event["event_ts"],
            json.dumps(event["payload"], default=str),
            run_id,
        )
        for event in events
    ]


def store_batch(rows: list[tuple]) -> None:
    if not rows:
        return
    connection = get_postgres_connection()
    try:
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
    finally:
        close_connection(connection)


def decode_valid(raw: bytes) -> dict[str, Any] | None:
    try:
        event = decode(raw)
    except ValueError:
        return None
    if validate_event(event):
        return None
    return event


def run() -> int:
    from src.utils.tracking import resolve_run_id

    topic = env("KAFKA_TOPIC_DELIVERY_EVENTS", "lh.delivery_events.v1")
    run_id = resolve_run_id()
    limit = batch_size()
    ensure_table()
    consumer = Consumer(consumer_config(env("CONSUMER_GROUP_PG_SINK", "lh-pg-sink")))
    consumer.subscribe([topic])
    events: list[dict[str, Any]] = []
    last: Message | None = None
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                if events:
                    store_batch(build_rows(events, run_id))
                    consumer.commit(message=last)
                    events = []
                continue
            if message.error():
                logger.warning("consumer error %s", message.error())
                continue
            event = decode_valid(message.value())
            if event is None:
                consumer.commit(message=message)
                continue
            events.append(event)
            last = message
            if len(events) >= limit:
                store_batch(build_rows(events, run_id))
                consumer.commit(message=last)
                events = []
    except KeyboardInterrupt:
        if events:
            store_batch(build_rows(events, run_id))
        return 0
    finally:
        consumer.close()


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001
        logger.error("postgres sink failed error=%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
