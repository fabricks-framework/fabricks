import os
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
LOCAL_STORAGE = REPO_ROOT / "framework" / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(REPO_ROOT / "framework")
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"

shutil.rmtree(LOCAL_STORAGE, ignore_errors=True)


def test_bronze_silver_gold_expected_resolve_to_local_storage():
    from fabricks.context import PATHS_STORAGE
    from fabricks.utils.path.local import LocalFileSharePath

    for name in ("bronze", "silver", "gold", "expected"):
        storage = PATHS_STORAGE.get(name)
        assert storage is not None, f"{name} not found in PATHS_STORAGE"
        assert isinstance(storage, LocalFileSharePath)
