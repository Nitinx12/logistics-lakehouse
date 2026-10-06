import os
import shutil
import subprocess
import sys
import threading
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
        "--master",
        os.getenv("SPARK_MASTER", "local[*]"),
        # Hyper-V reserves 3951-4495 here, swallowing the default 4040+ UI range
        "--conf",
        "spark.ui.port=18080",
        "--jars",
        ",".join(str(JARS_DIR / jar) for jar in jars),
        str(job_file),
        *args,
    ]


# points SPARK_HOME at the pip pyspark package when unset
def ensure_spark_home(env: dict) -> None:
    if env.get("SPARK_HOME"):
        return
    try:
        import pyspark

        home = Path(pyspark.__file__).resolve().parent
        if (home / "jars").is_dir():
            env["SPARK_HOME"] = str(home)
    except ImportError:
        pass


# JVM shutdown noise that carries no signal on Windows (locked temp jars)
_NOISE = (
    "ShutdownHookManager",
    "Failed to delete:",
    "at org.apache.spark",
    "at scala.",
    "at java.",
    "at sun.",
    "at jdk.",
    "WARNING: Using incubator modules",
)


# streams stderr live, dropping known JVM shutdown noise
def _stream_filtered(pipe) -> None:
    for line in iter(pipe.readline, ""):
        if not any(marker in line for marker in _NOISE):
            sys.stderr.write(line)
    pipe.close()


# runs one spark job and returns its exit code
def submit(job_file: Path, jars: list[str], args: list[str]) -> int:
    command = build_command(job_file, jars, args)
    env = dict(os.environ)
    ensure_spark_home(env)
    # spark workers must use this venv, or driver imports (psycopg) fail.
    # pinned, not setdefault: a bare PYSPARK_PYTHON=python in the shell is broken
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    logger.info("submit job=%s jars=%d", job_file.name, len(jars))
    process = subprocess.Popen(
        command,
        env=env,
        stdout=None,
        stderr=subprocess.PIPE,
        text=True,
    )
    watcher = threading.Thread(target=_stream_filtered, args=(process.stderr,))
    watcher.start()
    code = process.wait()
    watcher.join()
    logger.info("submit done job=%s code=%d", job_file.name, code)
    return code
