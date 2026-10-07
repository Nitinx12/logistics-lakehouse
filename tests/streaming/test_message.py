# Covers streaming envelope validation without needing Kafka.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.message import (  # noqa: E402
    build_event,
    decode,
    dlq_headers,
    encode,
    load_schema,
    validate_event,
)


def _doc() -> dict:
    return {
        "_id": "mongo-id",
        "event_id": "E1",
        "trip_id": "T1",
        "event_type": "Delivery",
        "actual_datetime": "2024-05-01T10:00:00Z",
        "scheduled_datetime": "2024-05-01T09:30:00Z",
        "on_time_flag": "False",
    }


def test_build_event_hides_mongo_id() -> None:
    message = build_event(_doc())
    assert message["event_id"] == "E1"
    assert message["trip_id"] == "T1"
    assert message["schema_version"] == 1
    assert "_id" not in message["payload"]
    assert validate_event(message) == ""


def test_validate_event_rejects_empty_key() -> None:
    message = build_event(_doc())
    message["trip_id"] = "  "
    assert "trip_id" in validate_event(message)


def test_validate_event_rejects_bad_timestamp() -> None:
    message = build_event(_doc())
    message["event_ts"] = "not-a-date"
    assert "event_ts" in validate_event(message)


def test_encode_decode_round_trip() -> None:
    message = build_event(_doc())
    assert decode(encode(message))["event_id"] == "E1"


def test_dlq_headers_carry_required_keys() -> None:
    headers = dlq_headers("bad timestamp", "lh.delivery_events.v1", 7)
    assert headers["error_reason"] == "bad timestamp"
    assert headers["original_topic"] == "lh.delivery_events.v1"
    assert headers["original_offset"] == "7"
    assert headers["failed_at"]


def test_schemas_exist_for_all_topics() -> None:
    for name in (
        "delivery_event.v1.json",
        "trip_status.v1.json",
        "delivery_alert.v1.json",
    ):
        assert load_schema(name)["title"]
