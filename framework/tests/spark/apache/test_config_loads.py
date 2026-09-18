from fabricks.context import PATHS_STORAGE
from fabricks.utils.path.local import LocalFileSharePath


def test_bronze_silver_gold_expected_resolve_to_local_storage():
    for name in ("bronze", "silver", "gold", "expected"):
        storage = PATHS_STORAGE.get(name)
        assert storage is not None, f"{name} not found in PATHS_STORAGE"
        assert isinstance(storage, LocalFileSharePath)
