# Covers procedure deploy ordering for the warehouse loaders.
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from run_procedures import PROCEDURE_FILES, list_procedures


def test_procedure_list_covers_all_loaders() -> None:
    assert len(list_procedures()) == 28


def test_procedure_files_all_exist() -> None:
    assert all(path.is_file() for path in PROCEDURE_FILES)


def test_masters_apply_after_their_tables() -> None:
    names = [path.name for path in PROCEDURE_FILES]
    assert names.index("proc_silver_load_mongo_all.sql") > names.index(
        "proc_silver_load_safety_incidents.sql"
    )
    assert names.index("proc_silver_load_databricks_all.sql") > names.index(
        "proc_silver_load_fuel_purchases.sql"
    )
    assert names.index("proc_gold_load_gold_all.sql") > names.index(
        "proc_gold_load_fact_safety_incidents.sql"
    )
