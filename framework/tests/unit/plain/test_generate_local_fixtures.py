import json

import pytest

# The one deliberate cross-tier import (see test_tier_boundary.py): the module must stay notebook-safe.
from tests.spark.databricks.fixtures import registered_delta_rows
from tests.support.fixture_data import (
    APACHE_FIXTURES_ROOT,
    ITERATIONS,
    RAW_FIXTURES_ROOT,
    derive_source_rows,
    validate_iteration,
    write_ndjson,
)

_ITER1_KING = RAW_FIXTURES_ROOT / "iter1" / "king"
_ITER1_QUEEN = RAW_FIXTURES_ROOT / "iter1" / "queen"


def test_derive_rows_from_iter1_king():
    rows = derive_source_rows(_ITER1_KING, source="king")

    assert len(rows) == 5

    first = rows[0]
    assert first["id"] == 1
    assert first["name"] == "Leopold I"
    assert first["__source"] == "king"
    assert first["__timestamp"] == "2022-01-01T00:01:00"


def test_registered_delta_rows_are_canonical_and_isolated():
    rows = registered_delta_rows("king")

    assert rows == [
        {"id": 1, "name": "Leopold I", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
        {"id": 2, "name": "Leopold II", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
    ]
    rows[0]["name"] = "changed"
    assert registered_delta_rows("king")[0]["name"] == "Leopold I"


def test_generated_ndjson_files_are_committed():
    king_path = APACHE_FIXTURES_ROOT / "iter1" / "bronze_king.jsonl"
    queen_path = APACHE_FIXTURES_ROOT / "iter1" / "bronze_queen.jsonl"

    assert king_path.exists()
    assert queen_path.exists()

    lines = king_path.read_text().strip().splitlines()
    # 5 main rows + 1 from the sibling iter1/king__deletelog directory
    assert len(lines) == 6
    row = json.loads(lines[0])
    assert row["__source"] == "king"
    assert row["__operation"] == "reload"
    assert not any(k.startswith("BEL_") for row in map(json.loads, lines) for k in row)
    assert any(json.loads(line)["__operation"] == "delete" for line in lines)


def test_derive_rows_is_deterministic_across_calls():
    # Committed to git: nondeterminism (dict or glob ordering) would show up as spurious diffs on regeneration.
    first = derive_source_rows(_ITER1_KING, source="king")
    second = derive_source_rows(_ITER1_KING, source="king")
    assert first == second


def test_derive_rows_order_matches_sorted_file_path_order():
    # A reordered batch changes which row wins a same-key dedup without changing the row count.
    rows = derive_source_rows(_ITER1_KING, source="king")
    timestamps = [r["__timestamp"] for r in rows]
    assert timestamps == sorted(timestamps)


def test_king_and_queen_rows_share_the_same_schema():
    # A key mismatch would otherwise surface only as a confusing Spark error when the entities are unioned by name.
    king_rows = derive_source_rows(_ITER1_KING, source="king")
    queen_rows = derive_source_rows(_ITER1_QUEEN, source="queen")

    king_keys = {frozenset(r.keys()) for r in king_rows}
    queen_keys = {frozenset(r.keys()) for r in queen_rows}
    assert king_keys == queen_keys, f"schema mismatch: king has {king_keys}, queen has {queen_keys}"


def test_write_ndjson_round_trips_without_loss(tmp_path):
    rows = derive_source_rows(_ITER1_KING, source="king")
    out_path = tmp_path / "king.jsonl"

    write_ndjson(rows, out_path)
    round_tripped = [json.loads(line) for line in out_path.read_text().splitlines()]

    assert round_tripped == rows


@pytest.mark.parametrize("iter_num", ITERATIONS)
def test_king_queen_jsonl_matches_concatenation_of_king_and_queen(iter_num):
    # king_queen.jsonl mimics the combined "monarch" topic; nothing enforces at write time that it matches the parts.
    iter_dir = APACHE_FIXTURES_ROOT / f"iter{iter_num}"
    king_queen_rows = [json.loads(line) for line in (iter_dir / "king_queen.jsonl").read_text().splitlines()]

    expected_rows = []
    for entity in ("king", "queen"):
        path = iter_dir / f"bronze_{entity}.jsonl"
        if (iter_num, entity) == (11, "queen"):  # generate_fixtures.py writes no file for an entity with no rows
            assert not path.exists()
            continue
        assert path.exists(), f"{path} is missing"
        expected_rows += [json.loads(line) for line in path.read_text().splitlines()]

    assert king_queen_rows == expected_rows


def test_regenerating_fixtures_twice_is_byte_identical(tmp_path):
    # Byte-identical, not just equal when parsed: json.dumps key ordering could drift unnoticed.
    rows = derive_source_rows(_ITER1_KING, source="king")

    first_path = tmp_path / "run1" / "king.jsonl"
    second_path = tmp_path / "run2" / "king.jsonl"
    write_ndjson(rows, first_path)
    write_ndjson(derive_source_rows(_ITER1_KING, source="king"), second_path)

    assert first_path.read_bytes() == second_path.read_bytes()


@pytest.mark.parametrize("iteration", ITERATIONS)
def test_committed_iteration_matches_canonical_data(iteration):
    validate_iteration(iteration)
