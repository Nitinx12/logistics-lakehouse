# Covers the topic table without needing Kafka.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.topics import topic_definitions  # noqa: E402


def test_four_versioned_topics_exist() -> None:
    definitions = topic_definitions()
    assert set(definitions) == {
        "lh.delivery_events.v1",
        "lh.delivery_events.v1.dlq",
        "lh.trip_status.v1",
        "lh.delivery_alerts.v1",
    }


def test_status_topic_is_compacted() -> None:
    assert topic_definitions()["lh.trip_status.v1"]["cleanup"] == "compact"


def test_events_keyed_by_trip() -> None:
    for name in ("lh.delivery_events.v1", "lh.trip_status.v1", "lh.delivery_alerts.v1"):
        assert topic_definitions()[name]["key"] == "trip_id"
