from dataclasses import dataclass, field
from pathlib import Path

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


@dataclass
class _Item:
    path: Path
    markers: list[str] = field(default_factory=list)

    def add_marker(self, marker) -> None:
        self.markers.append(marker.name)


def _item(relative: str) -> _Item:
    return _Item(path=Path(relative).resolve())


def test_mark_collected_tests_assigns_the_tier_marker():
    item = _item("tests/unit/plain/test_x.py")

    mark_collected_tests([item])

    assert item.markers == ["plain"]


def test_mark_collected_tests_leaves_paths_outside_tests_unmarked(tmp_path):
    item = _Item(path=tmp_path / "test_x.py")

    mark_collected_tests([item])

    assert item.markers == []


def test_mark_collected_tests_rejects_mixed_tiers():
    items = [_item("tests/unit/plain/test_x.py"), _item("tests/unit/config/test_y.py")]

    with pytest.raises(pytest.UsageError, match="cannot mix test tiers"):
        mark_collected_tests(items)


def test_activate_tier_is_idempotent_for_the_same_tier(monkeypatch):
    monkeypatch.delenv("FABRICKS_ACTIVE_TEST_TIER", raising=False)

    activate_tier("plain")
    activate_tier("plain")


def test_tier_for_path_ignores_node_id_suffix_and_unknown_paths():
    assert tier_for_path("tests/unit/config/test_x.py::test_y[param]") == "config"
    assert tier_for_path("/elsewhere/test_x.py") is None
