# Covers env driven Spark submit tuning with unchanged defaults.
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import _submit

TUNING_VARS = (
    "SPARK_MASTER",
    "SPARK_DRIVER_MEMORY",
    "SPARK_EXECUTOR_MEMORY",
    "SPARK_DRIVER_CORES",
    "SPARK_EXECUTOR_CORES",
    "SPARK_UI_PORT",
    "SPARK_SQL_SHUFFLE_PARTITIONS",
    "SPARK_SHUFFLE_PARTITIONS",
    "SPARK_LOCAL_IP",
    "SPARK_NETWORK_TIMEOUT",
    "SPARK_EXECUTOR_HEARTBEAT",
    "SPARK_EXTRA_CONFS",
)


@pytest.fixture
def command_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, list[str]]:
    for var in TUNING_VARS:
        monkeypatch.delenv(var, raising=False)
    jar = tmp_path / "dummy.jar"
    jar.write_bytes(b"")
    job = tmp_path / "job.py"
    job.write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setattr(_submit, "JARS_DIR", tmp_path)
    monkeypatch.setattr(_submit, "find_spark_submit", lambda: "spark-submit")
    return job, ["dummy.jar"]


def test_defaults_keep_previous_command(
    command_setup: tuple[Path, list[str]],
) -> None:
    job, jars = command_setup
    command = _submit.build_command(job, jars, [])
    assert command[0] == "spark-submit"
    assert "--master" in command
    assert "spark.ui.port=18080" in " ".join(command)
    assert "--driver-memory" not in command
    assert "--executor-memory" not in command


def test_driver_and_executor_memory_applied(
    command_setup: tuple[Path, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    job, jars = command_setup
    monkeypatch.setenv("SPARK_DRIVER_MEMORY", "1g")
    monkeypatch.setenv("SPARK_EXECUTOR_MEMORY", "1g")
    command = _submit.build_command(job, jars, [])
    assert "--driver-memory" in command
    assert command[command.index("--driver-memory") + 1] == "1g"
    assert "--executor-memory" in command


def test_local_ip_pins_driver_address(
    command_setup: tuple[Path, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    job, jars = command_setup
    monkeypatch.setenv("SPARK_LOCAL_IP", "10.0.0.5")
    joined = " ".join(_submit.build_command(job, jars, []))
    assert "spark.driver.host=10.0.0.5" in joined
    assert "spark.driver.bindAddress=10.0.0.5" in joined


def test_shuffle_partitions_prefers_canonical_name(
    command_setup: tuple[Path, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    job, jars = command_setup
    monkeypatch.setenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "200")
    joined = " ".join(_submit.build_command(job, jars, []))
    assert "spark.sql.shuffle.partitions=8" in joined


def test_network_tuning_and_extra_confs_applied(
    command_setup: tuple[Path, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    job, jars = command_setup
    monkeypatch.setenv("SPARK_NETWORK_TIMEOUT", "300s")
    monkeypatch.setenv("SPARK_EXECUTOR_HEARTBEAT", "30s")
    monkeypatch.setenv("SPARK_EXTRA_CONFS", "spark.rpc.numRetries=5, not-a-conf")
    joined = " ".join(_submit.build_command(job, jars, []))
    assert "spark.network.timeout=300s" in joined
    assert "spark.executor.heartbeatInterval=30s" in joined
    assert "spark.rpc.numRetries=5" in joined
