import os
import shutil
import subprocess
import sys
from pathlib import Path

# repo root on sys.path so `src` imports work from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
JARS_DIR = REPO_ROOT / "jars"


# resolves the spark-submit binary from PATH or the project venv
def find_spark_submit() -> str:
    found = shutil.which("spark-submit")
    if found:
        return found
    candidate = Path(sys.prefix) / "Scripts" / "spark-submit.cmd"
    if candidate.is_file():
        return str(candidate)
    raise FileNotFoundError("spark-submit not found, install pyspark first")


# builds the spark-submit command, failing fast on missing jars
def build_command(job_file: Path, jars: list[str], args: list[str]) -> list[str]:
    missing = [jar for jar in jars if not (JARS_DIR / jar).is_file()]
    if missing:
        raise FileNotFoundError(f"jars missing in {JARS_DIR}: {missing}")
    if not job_file.is_file():
        raise FileNotFoundError(f"job file missing: {job_file}")
    return [
        find_spark_submit(),
        "--master", os.getenv("SPARK_MASTER", "local[*]"),
        "--jars", ",".join(str(JARS_DIR / jar) for jar in jars),
        str(job_file),
        *args,
    ]


# runs one spark job and returns its exit code
def submit(job_file: Path, jars: list[str], args: list[str]) -> int:
    command = build_command(job_file, jars, args)
    logger.info("submit job=%s jars=%d", job_file.name, len(jars))
    completed = subprocess.run(command, check=False)
    logger.info("submit done job=%s code=%d", job_file.name, completed.returncode)
    return completed.returncode
