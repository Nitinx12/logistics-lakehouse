import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _submit import submit

from src.utils.tracking import track_stage

JOB_FILE = REPO_ROOT / "src" / "jobs" / "extract" / "mongo" / "mongo_to_postgres.py"
JARS = [
    "bson-5.1.4.jar",
    "bson-record-codec-5.1.4.jar",
    "mongo-spark-connector_2.12-10.5.0.jar",
    "mongodb-driver-core-5.1.4.jar",
    "mongodb-driver-sync-5.1.4.jar",
    "postgresql.jar",
]


# submits the mongo extract and records it in the run log
def main() -> int:
    with track_stage("extract_mongo"):
        code = submit(JOB_FILE, JARS, sys.argv[1:])
        if code != 0:
            raise SystemExit(code)
        return code


if __name__ == "__main__":
    sys.exit(main())
