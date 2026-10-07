import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# repo root on sys.path so `src` imports work from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.connection import close_connection, get_postgres_connection  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

# statuses accepted by bronze.etl_logs (chk_etl_logs_status)
VALID_STATUSES = ("STARTED", "SUCCESS", "FAILED")


# collapses a traceback into one readable line for console display
def short_error(exc: BaseException, max_len: int = 220) -> str:
    lines = str(exc).strip().splitlines()
    kept_lines: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("at ", 'File "')) or re.match(
            r"^\.{3}\s*\d+\s*more$", line
        ):
            break
        kept_lines.append(line)
        if len(kept_lines) >= 2:
            break
    message = (
        " -- ".join(kept_lines)
        if kept_lines
        else (lines or [exc.__class__.__name__])[0]
    )
    message = re.sub(r"\s+", " ", message).strip(": ")
    if len(message) > max_len:
        message = message[: max_len - 3].rstrip() + "..."
    return f"{exc.__class__.__name__}: {message}"


# renders a compact ASCII end-of-run summary, keys mirror bronze.etl_logs
def render_report(
    title: str,
    source: str,
    target_schema: str,
    run_id: str,
    dry_run: bool,
    results: list[dict],
    elapsed: float,
    unit: str,
    job_name: str,
    name_column: str = "Collection",
) -> bool:
    failed_results = [result for result in results if result.get("status") == "FAILED"]
    validation_failures = [
        result for result in results if result.get("validation") == "FAIL"
    ]
    skipped = sum(1 for result in results if result.get("status") == "SKIPPED")
    succeeded = sum(1 for result in results if result.get("status") == "SUCCESS")
    has_issues = bool(failed_results) or bool(validation_failures)
    mode_suffix = " dry-run" if dry_run else ""
    logger.info("%s%s", title, mode_suffix)
    logger.info("Source=%s TargetSchema=%s RunId=%s", source, target_schema, run_id)
    for result in results:
        logger.info(
            "%s=%s Status=%s Mode=%s Extracted=%s Loaded=%s Validation=%s Seconds=%.2f",
            name_column,
            result.get("target_table", "?"),
            result.get("status", "UNKNOWN"),
            result.get("mode", ""),
            f"{result.get('rows_extracted', 0):,}",
            f"{result.get('rows_loaded', 0):,}",
            result.get("validation", "n/a"),
            result.get("seconds", 0.0),
        )
        if result.get("error_message"):
            logger.error("Error=%s", result["error_message"])
    logger.info(
        "Summary %s=%d Succeeded=%d Skipped=%d Failed=%d ValidationFailures=%d Extracted=%s Loaded=%s Seconds=%.2f",
        unit,
        len(results),
        succeeded,
        skipped,
        len(failed_results),
        len(validation_failures),
        f"{sum(result.get('rows_extracted', 0) for result in results):,}",
        f"{sum(result.get('rows_loaded', 0) for result in results):,}",
        elapsed,
    )
    logger.info("Result=%s", "COMPLETED_WITH_ISSUES" if has_issues else "SUCCESS")
    return has_issues


# persists terminal rows into bronze.etl_logs, SKIPPED stays console-only
def write_etl_logs(results: list[dict], job_name: str) -> int:
    rows = [result for result in results if result.get("status") in VALID_STATUSES]
    if not rows:
        return 0
    now = datetime.now(timezone.utc)
    payload = [
        (
            result.get("job_name", job_name),
            result.get("target_table"),
            result.get("watermark_from"),
            result.get("watermark_to"),
            result.get("rows_extracted", 0),
            result.get("rows_loaded", 0),
            result.get("status"),
            result.get("error_message"),
            None if result.get("status") == "STARTED" else now,
        )
        for result in rows
    ]
    connection = get_postgres_connection()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO bronze.etl_logs "
                "(job_name, target_table, watermark_from, watermark_to, "
                "rows_extracted, rows_loaded, status, error_message, finished_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                payload,
            )
        connection.commit()
        logger.info("etl_logs wrote rows=%d", len(payload))
        return len(payload)
    except Exception:
        connection.rollback()
        raise
    finally:
        close_connection(connection)
