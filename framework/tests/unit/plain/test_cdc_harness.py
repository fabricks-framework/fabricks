import pytest

from tests.spark.apache.cdc_harness import validate_scenario
from tests.spark.test_data import apache_fixture_paths


@pytest.mark.parametrize(
    ("seed_from", "iters", "compare_to"),
    [(0, [1], 1), (3, [4, 5, 6, 7], 7), (10, [11], 11)],
)
def test_validate_scenario_accepts_contiguous_iterations(seed_from, iters, compare_to):
    validate_scenario(seed_from, iters, compare_to)


@pytest.mark.parametrize(
    ("seed_from", "iters", "compare_to", "message"),
    [
        (0, [], None, "must not be empty"),
        (1, [3], 3, "contiguous range"),
        (1, [2], 3, "own last element"),
    ],
)
def test_validate_scenario_rejects_invalid_ranges(seed_from, iters, compare_to, message):
    with pytest.raises(ValueError, match=message):
        validate_scenario(seed_from, iters, compare_to)


def test_fixture_paths_skip_missing_entities():
    assert [path.name for path in apache_fixture_paths(1)] == ["bronze_king.jsonl", "bronze_queen.jsonl"]
    assert [path.name for path in apache_fixture_paths(11)] == ["bronze_king.jsonl"]
