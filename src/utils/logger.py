import logging
import os
from pathlib import Path

# single-line format shared by every module logger
_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_configured = False


# walks up to the project root (pyproject.toml) so logs land in one place
def _project_root() -> Path:
    current = Path.cwd().resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return current


# returns a module logger, console plus file output configured once
def get_logger(name: str) -> logging.Logger:
    global _configured
    if not _configured:
        level = getattr(
            logging, (os.getenv("LOG_LEVEL") or "INFO").upper(), logging.INFO
        )
        formatter = logging.Formatter(_FORMAT)
        root = logging.getLogger()
        root.setLevel(level)
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)
        log_dir = Path(os.getenv("LOG_DIR") or "logs")
        if not log_dir.is_absolute():
            log_dir = _project_root() / log_dir
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_dir / "lakehouse.log")
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except OSError:
            root.warning("log file unavailable, console only")
        _configured = True
    return logging.getLogger(name)
