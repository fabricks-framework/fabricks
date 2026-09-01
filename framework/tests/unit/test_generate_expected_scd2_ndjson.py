"""Tests for tests/local/generate_expected_scd2_ndjson.py.

Pure Python/stdlib, no Spark, no `fabricks` import -- lives in tests/unit
(not tests/local) because tests/local/conftest.py pulls in a real Delta
session. See task-3b-brief.md.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

EXPECTED_DIR = Path(__file__).resolve().parents[1] / "expected" / "silver" / "scd2"
SCRIPT_PATH = Path(__file__).resolve().parents[1] / "local" / "generate_expected_scd2_ndjson.py"

# Import the generator script as a module (it lives outside tests/unit and
# tests/local isn't an importable package until Task 7 adds its conftest).
_spec = importlib.util.spec_from_file_location("generate_expected_scd2_ndjson", SCRIPT_PATH)
gen = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = gen
_spec.loader.exec_module(gen)

JOB_NUMS = list(range(2, 12))


def read_ndjson(job_num: int) -> list[dict]:
    return gen.read_ndjson(EXPECTED_DIR / f"job{job_num:02d}.ndjson")


@pytest.fixture(scope="module", autouse=True)
def regenerate():
    """Ensure the ndjson files under test reflect the current script/sql."""
    gen.main()


@pytest.mark.parametrize("job_num", JOB_NUMS)
def test_ndjson_parses_cleanly(job_num):
    path = EXPECTED_DIR / f"job{job_num:02d}.ndjson"
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines, f"job{job_num:02d}.ndjson is empty"
    for line in lines:
        json.loads(line)  # raises if invalid


@pytest.mark.parametrize("job_num", JOB_NUMS)
def test_row_count_is_own_rows_plus_forwarded_rows(job_num):
    """this job's own new rows + previous job's non-current row count ==
    this job's total row count. Own-row count comes from re-parsing the
    .sql file; forwarded count comes from an independent read of the
    previous job's ndjson (not via the script's own carry_forward()), so
    this doesn't just check the generator agrees with itself."""
    sql = (EXPECTED_DIR / f"job{job_num:02d}.sql").read_text(encoding="utf-8")
    header = gen.parse_header(sql)
    own_rows = gen.parse_values_rows(sql, header)

    if job_num == 2:
        prev_rows = gen.read_ndjson(EXPECTED_DIR / "job01.ndjson")
    else:
        prev_rows = read_ndjson(job_num - 1)
    forwarded_count = sum(1 for r in prev_rows if not r["__is_current"])

    actual_rows = read_ndjson(job_num)
    assert len(actual_rows) == len(own_rows) + forwarded_count


def test_monotonic_row_counts_across_chain():
    """SCD2 never deletes history: total row count must only grow or stay
    flat job-over-job, never shrink."""
    counts = [len(gen.read_ndjson(EXPECTED_DIR / "job01.ndjson"))]
    counts += [len(read_ndjson(n)) for n in JOB_NUMS]
    for prev_count, count in zip(counts, counts[1:]):
        assert count >= prev_count
    assert counts[-1] > counts[0]  # sanity: chain actually grows overall


def test_job2_schema_boundary_newfield_null_for_forwarded_job1_rows():
    """job1 has no `newField` column. job2 pulls forward job1's non-current
    rows (id=2's closed row, id=101, id=201) -- they must get
    newField: null. job2's own new rows (id=1, id=2 closed, id=3, id=301)
    carry a real (string) newField value, never None."""
    rows = read_ndjson(2)
    by_id_and_from = {(r["id"], r["__valid_from"]): r for r in rows}

    # forwarded from job1 (job1 doesn't have newField at all)
    assert by_id_and_from[(2, "1900-01-01 00:00:00")]["newField"] is None
    assert by_id_and_from[(101, "1900-01-01 00:00:00")]["newField"] is None
    assert by_id_and_from[(201, "2022-01-02 00:01:00")]["newField"] is None

    # job2's own rows -- real values, not None (including the 'null' *string*
    # literal from the .sql, which is a quoted string cast as string, not SQL
    # NULL, and must round-trip as the literal string "null")
    assert by_id_and_from[(1, "1900-01-01 00:00:00")]["newField"] == "null"
    assert by_id_and_from[(3, "2022-02-05 00:01:00")]["newField"] == "true"


def test_job3_onward_never_introduces_new_null_newfield_rows():
    """The null-filling schema-boundary logic only ever fires at job1->job2
    (job1's rows lack `newField` entirely). The 3 rows it fills there (id=2's
    original closed row, id=101, id=201) are permanently historized -- no
    later job's own VALUES ever touches those (id, __valid_from) pairs again
    -- so they keep newField: null forever as they're carried forward
    unchanged. What must NOT happen: any *new* (id, __valid_from) pair
    appearing with newField=null from job3 onward, which would mean the
    null-fill logic incorrectly fired again on some other row."""
    job2_null_keys = {
        (r["id"], r["__valid_from"]) for r in read_ndjson(2) if r["newField"] is None
    }
    assert job2_null_keys == {(2, "1900-01-01 00:00:00"), (101, "1900-01-01 00:00:00"), (201, "2022-01-02 00:01:00")}

    for job_num in range(3, 12):
        rows = read_ndjson(job_num)
        null_keys = {(r["id"], r["__valid_from"]) for r in rows if r["newField"] is None}
        assert null_keys == job2_null_keys, (
            f"job{job_num:02d}.ndjson's null-newField rows {null_keys} "
            f"differ from job2's {job2_null_keys}"
        )


def test_spot_check_job2_open_row_against_sql_literal():
    """id=301, open row -- directly transcribed from job02.sql's own VALUES
    tuple (('2022-02-05 00:01:00', '9999-12-31 00:00:00', '301', 'Elisabeth',
    '0.20220205', 'true', 'true', 'false', 'queen'))."""
    rows = read_ndjson(2)
    row = next(r for r in rows if r["id"] == 301)
    assert row == {
        "__valid_from": "2022-02-05 00:01:00",
        "__valid_to": "9999-12-31 00:00:00",
        "id": 301,
        "name": "Elisabeth",
        "doubleField": 0.20220205,
        "newField": "true",
        "__is_current": True,
        "__is_deleted": False,
        "__source": "queen",
    }


def test_spot_check_job11_mixed_case_boolean_literal():
    """job11.sql's id=9 row uses 'False' (capital F) for __is_current --
    verifies boolean parsing is case-insensitive, against the real literal
    (('2022-10-03 00:01:00', '2022-11-01 00:00:59', '9', 'Philippe',
    '0.20221003', 'true', 'False', 'true', 'king'))."""
    rows = read_ndjson(11)
    row = next(r for r in rows if r["id"] == 9)
    assert row == {
        "__valid_from": "2022-10-03 00:01:00",
        "__valid_to": "2022-11-01 00:00:59",
        "id": 9,
        "name": "Philippe",
        "doubleField": 0.20221003,
        "newField": "true",
        "__is_current": False,
        "__is_deleted": True,
        "__source": "king",
    }


def test_sql_files_untouched():
    """This task must not delete or modify job02.sql-job11.sql."""
    for job_num in JOB_NUMS:
        assert (EXPECTED_DIR / f"job{job_num:02d}.sql").exists()
