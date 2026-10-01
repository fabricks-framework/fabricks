"""validate_scenario: the argument contract of run_cdc_scenario, and which king/queen fixture files exist."""

import pytest

from tests.support.cdc_scenario import validate_scenario
from tests.support.fixture_data import apache_fixture_paths


@pytest.mark.parametrize(
    ("seed_from", "iters", "compare_to"),
    [
        pytest.param(0, [1], 1, id="first-iteration"),
        pytest.param(3, [4, 5, 6, 7], 7, id="seeded-range"),
        pytest.param(10, [11], 11, id="last-iteration"),
        pytest.param(0, [1], None, id="compare-to-defaults-to-none"),
    ],
)
def test_validate_scenario_accepts_contiguous_iterations(seed_from, iters, compare_to):
    validate_scenario(seed_from, iters, compare_to)


@pytest.mark.parametrize(
    ("seed_from", "iters", "compare_to", "message"),
    [
        pytest.param(0, [], None, "must not be empty", id="empty"),
        pytest.param(1, [3], 3, "contiguous range", id="gap"),
        pytest.param(1, [2], 3, "own last element", id="compare-to-mismatch"),
    ],
)
def test_validate_scenario_rejects_invalid_ranges(seed_from, iters, compare_to, message):
    with pytest.raises(ValueError, match=message):
        validate_scenario(seed_from, iters, compare_to)


def test_fixture_paths_skip_missing_entities():
    assert [path.name for path in apache_fixture_paths(1)] == ["bronze_king.jsonl", "bronze_queen.jsonl"]
    assert [path.name for path in apache_fixture_paths(11)] == ["bronze_king.jsonl"]
