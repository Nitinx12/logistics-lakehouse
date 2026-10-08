# Submits PySpark jobs with env driven memory and network tuning.
import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path

# repo root on sys.path so `src` imports work from any working directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.logger import get_logger

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


# returns a stripped env value or an empty string when unset
def _nonempty(name: str) -> str:
    return (os.getenv(name) or "").strip()


# collects optional spark confs, empty when no tuning env is set
def _spark_confs() -> list[str]:
    confs: list[str] = []
    ui_port = _nonempty("SPARK_UI_PORT") or "18080"
    confs += ["--conf", f"spark.ui.port={ui_port}"]
    partitions = _nonempty("SPARK_SQL_SHUFFLE_PARTITIONS") or _nonempty(
        "SPARK_SHUFFLE_PARTITIONS"
    )
    if partitions:
        confs += ["--conf", f"spark.sql.shuffle.partitions={partitions}"]
    local_ip = _nonempty("SPARK_LOCAL_IP")
    if local_ip:
        confs += [
            "--conf",
            f"spark.driver.host={local_ip}",
            "--conf",
            f"spark.driver.bindAddress={local_ip}",
        ]
    timeout = _nonempty("SPARK_NETWORK_TIMEOUT")
    if timeout:
        confs += ["--conf", f"spark.network.timeout={timeout}"]
    heartbeat = _nonempty("SPARK_EXECUTOR_HEARTBEAT")
    if heartbeat:
        confs += ["--conf", f"spark.executor.heartbeatInterval={heartbeat}"]
    extra = _nonempty("SPARK_EXTRA_CONFS")
    if extra:
        for item in (part.strip() for part in extra.split(",")):
            if item and "=" in item:
                confs += ["--conf", item]
    return confs


# builds the spark-submit command, failing fast on missing jars
def build_command(job_file: Path, jars: list[str], args: list[str]) -> list[str]:
    missing = [jar for jar in jars if not (JARS_DIR / jar).is_file()]
    if missing:
        raise FileNotFoundError(f"jars missing in {JARS_DIR}: {missing}")
    if not job_file.is_file():
        raise FileNotFoundError(f"job file missing: {job_file}")
    command = [
        find_spark_submit(),
        "--master",
        os.getenv("SPARK_MASTER", "local[*]"),
    ]
    driver_memory = _nonempty("SPARK_DRIVER_MEMORY")
    if driver_memory:
        command += ["--driver-memory", driver_memory]
    executor_memory = _nonempty("SPARK_EXECUTOR_MEMORY")
    if executor_memory:
        command += ["--executor-memory", executor_memory]
    driver_cores = _nonempty("SPARK_DRIVER_CORES")
    if driver_cores:
        command += ["--driver-cores", driver_cores]
    executor_cores = _nonempty("SPARK_EXECUTOR_CORES")
    if executor_cores:
        command += ["--executor-cores", executor_cores]
    command += _spark_confs()
    return [
        *command,
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
