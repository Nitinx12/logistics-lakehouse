# Shared DAG plumbing so the seven DAGs stay thin (ARCHITECTURE.md §9.4).
import os


def repo_dir() -> str:
    return os.getenv("LAKEHOUSE_REPO", "/app")


def task_env() -> dict[str, str]:
    return {"LAKEHOUSE_REPO": repo_dir(), "WAREHOUSE_RUN_ID": "{{ run_id }}"}


def script_command(script: str) -> str:
    return f'cd "$LAKEHOUSE_REPO" && uv run --frozen {script}'


def snapshot_command() -> str:
    return (
        'cd "$LAKEHOUSE_REPO" && uv run python -c '
        '"from src.utils.tracking import record_snapshot; record_snapshot()"'
    )
