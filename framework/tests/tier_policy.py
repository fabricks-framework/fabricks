from pathlib import Path

import pytest

from tests.spark.databricks.tier_policy import Tier, activate_tier  # noqa: F401

_TESTS_ROOT = Path(__file__).parent.resolve()
_PATH_TIERS: tuple[tuple[tuple[str, ...], Tier], ...] = (
    (("unit", "plain"), "plain"),
    (("unit", "config"), "config"),
    (("spark", "apache"), "apache"),
    (("spark", "databricks"), "databricks"),
)


def tier_for_path(path: str | Path) -> Tier | None:
    candidate = Path(str(path).split("::", 1)[0])
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    try:
        parts = candidate.resolve().relative_to(_TESTS_ROOT).parts
    except ValueError:
        return None

    for prefix, tier in _PATH_TIERS:
        if parts[: len(prefix)] == prefix:
            return tier
    return None


def mark_collected_tests(items) -> None:
    tiers = set()
    for item in items:
        tier = tier_for_path(item.path)
        if tier:
            item.add_marker(getattr(pytest.mark, tier))
            tiers.add(tier)
    if len(tiers) > 1:
        raise pytest.UsageError("cannot mix test tiers; run each tier with its separate just command")
