# Builds and validates streaming envelopes (ARCHITECTURE.md §8.4).
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"

REQUIRED_EVENT_FIELDS = (
    "event_id",
    "trip_id",
    "event_type",
    "event_ts",
    "payload",
    "schema_version",
    "source",
    "produced_at",
)

DLQ_HEADERS = ("error_reason", "original_topic", "original_offset", "failed_at")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_event(
    doc: dict[str, Any],
    event_ts_field: str = "actual_datetime",
    source: str = "replay-producer",
) -> dict[str, Any]:
    payload = {key: value for key, value in doc.items() if key != "_id"}
    return {
        "event_id": str(doc.get("event_id", "")),
        "trip_id": str(doc.get("trip_id", "")),
        "event_type": str(doc.get("event_type", "")),
        "event_ts": str(doc.get(event_ts_field, "")),
        "payload": payload,
        "schema_version": 1,
        "source": source,
        "produced_at": utc_now_iso(),
    }


def validate_event(message: dict[str, Any]) -> str:
    for field in REQUIRED_EVENT_FIELDS:
        if field not in message:
            return f"missing field {field}"
        value = message[field]
        if isinstance(value, str) and not value.strip():
            return f"empty field {field}"
    if not isinstance(message["payload"], dict):
        return "payload must be an object"
    if message["schema_version"] != 1:
        return "unsupported schema_version"
    for field in ("event_ts", "produced_at"):
        try:
            parse_iso(str(message[field]))
        except ValueError:
            return f"bad timestamp {field}"
    return ""


def encode(message: dict[str, Any]) -> bytes:
    return json.dumps(message, default=str).encode("utf-8")


def decode(raw: bytes) -> dict[str, Any]:
    loaded = json.loads(raw.decode("utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("message must be a JSON object")
    return loaded


def dlq_headers(reason: str, topic: str, offset: int | str) -> dict[str, str]:
    return {
        "error_reason": reason[:256],
        "original_topic": topic,
        "original_offset": str(offset),
        "failed_at": utc_now_iso(),
    }


def load_schema(name: str) -> dict[str, Any]:
    return json.loads((SCHEMA_DIR / name).read_text(encoding="utf-8"))
