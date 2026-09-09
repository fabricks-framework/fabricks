from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.tier_policy import activate_tier, mark_collected_tests, tier_for_path


@pytest.mark.parametrize(
    ("path", "tier"),
    [
        ("tests/unit/plain/test_x.py", "plain"),
        ("tests/unit/config/test_x.py", "config"),
        ("tests/spark/apache/test_x.py", "apache"),
        ("tests/spark/databricks/test_x.py", "databricks"),
    ],
)
def test_tier_for_path(path, tier):
    assert tier_for_path(path) == tier


def test_activate_tier_rejects_mixed_process(monkeypatch):
    monkeypatch.delenv("FABRICKS_ACTIVE_TEST_TIER", raising=False)
    activate_tier("plain")

    with pytest.raises(pytest.UsageError, match="cannot mix plain and config"):
        activate_tier("config")


def test_mark_collected_tests_assigns_path_marker():
    item = MagicMock(path=Path("tests/unit/plain/test_x.py").resolve())

    mark_collected_tests([item])

    item.add_marker.assert_called_once()
