# Covers the stream table exemption in batch bronze checks.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

STREAM_ONLY_CHECKS = (
    "01_lp_check_bronze_tables.sql",
    "03_lp_check_bronze_loaded_at.sql",
    "05_lp_check_bronze_raw_text.sql",
    "06_lp_check_bronze_extraction_health.sql",
    "07_lp_check_bronze_ingestion_marker.sql",
)


def test_stream_table_exempt_from_batch_checks() -> None:
    bronze = REPO_ROOT / "tests" / "dq" / "bronze"
    for filename in STREAM_ONLY_CHECKS:
        text = (bronze / filename).read_text(encoding="utf-8")
        assert "delivery_events_stream" in text, filename
