import sys
from pathlib import Path

# shared console reporter lives in scripts/, bridge old result keys to it
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _report import render_report as _base_render
from _report import short_error  # noqa: F401


# adapts legacy job result dicts to the shared reporter keys
def _normalize(result: dict) -> dict:
    normalized = dict(result)
    normalized.setdefault("target_table", result.get("name", "?"))
    normalized.setdefault(
        "rows_extracted",
        result.get("rows_extracted", result.get("total", result.get("extracted", 0))),
    )
    normalized.setdefault(
        "rows_loaded",
        result.get("rows_loaded", result.get("inserted", 0) + result.get("updated", 0)),
    )
    normalized.setdefault(
        "error_message", result.get("error_message", result.get("error"))
    )
    return normalized


# renders the end-of-run report from legacy result dicts
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
    return _base_render(
        title,
        source,
        target_schema,
        run_id,
        dry_run,
        [_normalize(result) for result in results],
        elapsed,
        unit,
        job_name,
        name_column,
    )
