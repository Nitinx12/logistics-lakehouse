# Warehouse task entry points for Airflow BashOperators.
import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "airflow") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "airflow"))


def cmd_preflight(args: argparse.Namespace) -> int:
    from include.ops import check_preflight

    check_preflight(*args.systems)
    return 0


def cmd_read_watermarks(_args: argparse.Namespace) -> int:
    from include.ops import read_watermarks

    print(read_watermarks())
    return 0


def cmd_reconcile(_args: argparse.Namespace) -> int:
    from include.ops import check_reconcile

    check_reconcile()
    return 0


def cmd_freshness(_args: argparse.Namespace) -> int:
    from include.ops import update_freshness

    update_freshness()
    return 0


def cmd_advance(args: argparse.Namespace) -> int:
    from include.ops import advance_watermarks

    from src.utils.tracking import resolve_run_id

    advance_watermarks(args.batch_id or resolve_run_id())
    return 0


def cmd_maintenance(_args: argparse.Namespace) -> int:
    from include.ops import run_analyze, run_retention

    run_retention()
    run_analyze()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse ops tasks for Airflow")
    sub = parser.add_subparsers(dest="command", required=True)
    preflight = sub.add_parser("preflight")
    preflight.add_argument("--systems", nargs="+", default=["postgres"])
    preflight.set_defaults(func=cmd_preflight)
    reader = sub.add_parser("read-watermarks")
    reader.set_defaults(func=cmd_read_watermarks)
    reconcile = sub.add_parser("reconcile")
    reconcile.set_defaults(func=cmd_reconcile)
    freshness = sub.add_parser("freshness")
    freshness.set_defaults(func=cmd_freshness)
    advance = sub.add_parser("advance")
    advance.add_argument("--batch-id", default="")
    advance.set_defaults(func=cmd_advance)
    maintenance = sub.add_parser("maintenance")
    maintenance.set_defaults(func=cmd_maintenance)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
