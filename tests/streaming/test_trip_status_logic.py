# Covers keyed stream logic without needing Kafka.
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.flink.logic import (
    advance_watermark,
    delay_alert,
    delay_minutes,
    is_late,
    late_alert,
    stuck_alerts,
    to_status,
)
from streaming.flink.trip_status_job import (
    load_checkpoint,
    save_checkpoint,
)
from streaming.message import parse_iso


def _message(flag: str = "False") -> dict:
    return {
        "event_id": "E1",
        "trip_id": "T1",
        "event_type": "Delivery",
        "event_ts": "2024-05-01T10:00:00Z",
        "payload": {
            "scheduled_datetime": "2024-05-01T09:30:00Z",
            "actual_datetime": "2024-05-01T10:00:00Z",
            "on_time_flag": flag,
        },
        "schema_version": 1,
        "source": "replay-producer",
        "produced_at": "2024-05-01T10:00:01Z",
    }


def test_delay_minutes_compares_planned_vs_actual() -> None:
    assert delay_minutes(_message()) == 30.0


def test_to_status_keeps_latest_event() -> None:
    status = to_status(_message())
    assert status["trip_id"] == "T1"
    assert status["status"] == "Delivery"
    assert status["delay_minutes"] == 30.0


def test_delay_alert_fires_only_on_late_flag() -> None:
    assert delay_alert(_message("False"))["alert_type"] == "delay"
    assert delay_alert(_message("True")) is None


def test_is_late_respects_allowed_lateness() -> None:
    watermark = parse_iso("2024-05-01T10:05:00Z")
    event_ts = parse_iso("2024-05-01T10:00:00Z")
    assert is_late(event_ts, watermark, 120.0) is True
    assert is_late(event_ts, watermark, 400.0) is False
    assert late_alert(_message())["alert_type"] == "late_data"


def test_stuck_alerts_fire_after_silence() -> None:
    now = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
    seen = {"T1": datetime(2024, 5, 1, 10, 0, tzinfo=UTC)}
    alerts = stuck_alerts(seen, now, 30.0)
    assert [alert["trip_id"] for alert in alerts] == ["T1"]
    assert stuck_alerts({"T1": now}, now, 30.0) == []


def test_watermark_never_moves_backwards() -> None:
    high = parse_iso("2024-05-01T10:05:00Z")
    low = parse_iso("2024-05-01T10:00:00Z")
    assert advance_watermark(high, low) == high
    assert advance_watermark(None, low) == low


def test_checkpoint_round_trip(tmp_path) -> None:
    path = tmp_path / "trip_status.json"
    seen = {"T1": datetime(2024, 5, 1, 10, 0, tzinfo=UTC)}
    save_checkpoint(path, seen, parse_iso("2024-05-01T10:05:00Z"))
    loaded, current_max = load_checkpoint(path)
    assert loaded == seen
    assert current_max == parse_iso("2024-05-01T10:05:00Z")


def test_checkpoint_missing_or_corrupt_starts_empty(tmp_path) -> None:
    assert load_checkpoint(tmp_path / "absent.json") == ({}, None)
    broken = tmp_path / "broken.json"
    broken.write_text("{nope", encoding="utf-8")
    assert load_checkpoint(broken) == ({}, None)
