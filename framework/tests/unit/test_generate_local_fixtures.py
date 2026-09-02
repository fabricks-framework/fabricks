import json
from pathlib import Path

from tests.local.generate_fixtures import derive_rows, write_ndjson

_REPO_ROOT = Path(__file__).resolve().parents[3]
_JOB1_KING = _REPO_ROOT / "framework" / "tests" / "data" / "job1" / "king"
_JOB1_QUEEN = _REPO_ROOT / "framework" / "tests" / "data" / "job1" / "queen"


def test_derive_rows_from_job1_king():
    rows = derive_rows(_JOB1_KING, source="king")

    # job1/king has 2 batches (2022/01/01/0001 with 3 rows incl. a duplicate,
    # 2022/01/02/0001 with 2 rows) — see the raw fixture files.
    assert len(rows) == 5

    first = rows[0]
    assert first["id"] == 1
    assert first["name"] == "Leopold I"
    assert first["__source"] == "king"
    assert first["__timestamp"] == "2022-01-01T00:01:00"


def test_generated_ndjson_files_are_committed():
    king_path = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures" / "job1" / "bronze_king_scd1.jsonl"
    queen_path = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures" / "job1" / "bronze_queen_scd1.jsonl"

    assert king_path.exists()
    assert queen_path.exists()

    lines = king_path.read_text().strip().splitlines()
    assert len(lines) == 5
    row = json.loads(lines[0])
    assert row["__source"] == "king"


def test_derive_rows_is_deterministic_across_calls():
    # Same call, twice, must produce byte-for-byte identical rows — this
    # output gets committed to git, so any source of nondeterminism (dict
    # ordering, unstable glob ordering across filesystems/OSes) would show
    # up as spurious diffs every time someone regenerates the fixtures.
    first = derive_rows(_JOB1_KING, source="king")
    second = derive_rows(_JOB1_KING, source="king")
    assert first == second


def test_derive_rows_order_matches_sorted_file_path_order():
    # derive_rows sorts entity_dir.rglob("*.json") before reading, so row
    # order should follow the YYYY/MM/DD/NNNN path order chronologically —
    # batch 2022/01/01/0001's 3 rows (incl. a duplicate) before batch
    # 2022/01/02/0001's 1 row. Order matters here: king_and_queen_built
    # (Task 9) feeds these rows straight into NoCDC.overwrite() as one
    # DataFrame, and a silently reordered batch would change which row
    # "wins" a same-key dedup without changing the row count, so a count-only
    # assertion wouldn't catch it.
    rows = derive_rows(_JOB1_KING, source="king")
    timestamps = [r["__timestamp"] for r in rows]
    assert timestamps == sorted(timestamps)


def test_king_and_queen_rows_share_the_same_schema():
    # Task 9's king_and_queen_built fixture unions king's and queen's
    # DataFrames (unionByName). A key mismatch between the two entities'
    # derived rows would only surface there as a confusing Spark error —
    # catching it here, at the pure-Python level, is cheaper and clearer.
    king_rows = derive_rows(_JOB1_KING, source="king")
    queen_rows = derive_rows(_JOB1_QUEEN, source="queen")

    king_keys = {frozenset(r.keys()) for r in king_rows}
    queen_keys = {frozenset(r.keys()) for r in queen_rows}
    assert king_keys == queen_keys, f"schema mismatch: king has {king_keys}, queen has {queen_keys}"


def test_write_ndjson_round_trips_without_loss(tmp_path):
    rows = derive_rows(_JOB1_KING, source="king")
    out_path = tmp_path / "king.jsonl"

    write_ndjson(rows, out_path)
    round_tripped = [json.loads(line) for line in out_path.read_text().splitlines()]

    assert round_tripped == rows


def test_regenerating_fixtures_twice_is_byte_identical(tmp_path):
    # The strongest form of "consistent across iterations": running the
    # full generation twice into two separate directories must produce
    # byte-for-byte identical files, not just equal-when-parsed rows —
    # dict key ordering inside json.dumps() is one way this could silently
    # drift even if test_derive_rows_is_deterministic_across_calls passes.
    rows = derive_rows(_JOB1_KING, source="king")

    first_path = tmp_path / "run1" / "king.jsonl"
    second_path = tmp_path / "run2" / "king.jsonl"
    write_ndjson(rows, first_path)
    write_ndjson(derive_rows(_JOB1_KING, source="king"), second_path)

    assert first_path.read_bytes() == second_path.read_bytes()
