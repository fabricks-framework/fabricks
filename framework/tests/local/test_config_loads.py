import os
import shutil
from pathlib import Path

FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]
LOCAL_STORAGE = FRAMEWORK_ROOT / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(FRAMEWORK_ROOT)
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
