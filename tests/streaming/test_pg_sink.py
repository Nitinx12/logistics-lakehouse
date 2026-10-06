# Covers sink batch row building without needing Kafka or Postgres.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.message import build_event  # noqa: E402
from streaming.sinks.postgres_sink import build_rows  # noqa: E402


def _doc(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "trip_id": "T1",
        "event_type": "Delivery",
        "actual_datetime": "2024-05-01T10:00:00Z",
        "scheduled_datetime": "2024-05-01T09:30:00Z",
        "on_time_flag": "True",
    }


def test_build_rows_maps_envelope_to_columns() -> None:
    events = [build_event(_doc("E1")), build_event(_doc("E2"))]
    rows = build_rows(events, "batch_1")
    assert len(rows) == 2
    assert rows[0][:4] == ("E1", "T1", "Delivery", "2024-05-01T10:00:00Z")
    assert rows[0][5] == "batch_1"


def test_build_rows_empty_in_empty_out() -> None:
    assert build_rows([], "batch_1") == []
