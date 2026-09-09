"""Real DAG ordering, forced-failure/skip bookkeeping, real fabricks.last_schedule state.

Runs runtime's tag="test" schedule once (via fabricks.core.schedules.standalone,
the same generate -> process (per step, real dbutils.notebook.run) -> terminate
sequence fabricks_run_job's tasks in databricks.yml drive), then asserts against
the resulting fabricks.last_schedule catalog state.

One test per runtime job/feature (see its README's table) -- each tagged
job gets exactly one assertion naming what it proves, rather than one test
per SQL query shape.
"""

import pytest

from fabricks.context import SPARK
from fabricks.core.schedules import standalone

EXPECTED_FAILURE = "gold.check_fail"
EXPECTED_SKIP = "gold.check_skip"
EXPECTED_WARNING = "gold.check_warning"


@pytest.fixture(scope="session", autouse=True)
def _schedule_run():
    standalone(schedule="test")


def _row(job: str):
    return SPARK.sql(f"select * from fabricks.last_schedule where job = '{job}'").collect()[0]


def _succeeded(job: str) -> bool:
    row = _row(job)
    return not row.failed and not row.skipped


def test_bronze_register_mode_king():
    # bronze.king_scd1: external-table registration against its seeded Delta
    # table -- the dummy-parser plugin itself is proven separately, direct-
    # invoke, by test_feature.py's test_bronze_feature_parser.
    assert _succeeded("bronze.king_scd1")


def test_bronze_register_mode_regent():
    assert _succeeded("bronze.regent_scd1")


def test_bronze_register_mode_queen():
    assert _succeeded("bronze.queen_scd1")


def test_cross_layer_dependency():
    # silver.king_scd1 (parents: [bronze.king_scd1]) only runs after its parent.
    assert _row("bronze.king_scd1").end_time < _row("silver.king_scd1").start_time


def test_auto_detected_gold_dependency():
    # gold.fact_dependency's SQL joins gold.dim_time + silver.king_scd1__current;
    # both parents are auto-detected via sqlglot, no wait_for needed.
    assert _row("gold.dim_time").end_time < _row("gold.fact_dependency").start_time
    assert _row("silver.king_scd1").end_time < _row("gold.fact_dependency").start_time


def test_manual_wait_for():
    # transf.fact_wait_for: wait_for=[transf.fact_memory, silver.king_scd1]
    assert _row("transf.fact_memory").end_time < _row("transf.fact_wait_for").start_time


def test_forced_failure():
    assert _row(EXPECTED_FAILURE).failed


def test_forced_skip():
    assert _row(EXPECTED_SKIP).skipped


def test_forced_warning():
    # fabricks/core/dags/run.py catches CheckWarning separately from a plain
    # Exception and logs 'warned', not 'failed' -- logs_pivot's own
    # `done = array_contains(statuses, 'done') or warned` means a warned job
    # reads as failed=False here, same as a clean run. for_each_run() still
    # executes before the warning is raised (Processor.run() only raises it
    # at the very end), so the table is populated despite the warning.
    row = _row(EXPECTED_WARNING)
    assert row.warned
    assert not row.failed
    assert SPARK.sql(f"select count(*) from {EXPECTED_WARNING}").collect()[0][0] == 1


def test_custom_view():
    # fabricks.dummy (fabricks/views/dummy.sql): create_or_replace_views()
    # deploys custom views from PATH_VIEWS at armageddon time.
    df = SPARK.sql("select * from fabricks.dummy")
    assert df.count() > 0


def test_no_unforced_failures():
    df = SPARK.sql(f"select * from fabricks.last_schedule where failed and job != '{EXPECTED_FAILURE}'")
    assert df.count() == 0, "unforced failure <> 0"


def test_no_unforced_skips():
    df = SPARK.sql(f"select * from fabricks.last_schedule where skipped and job != '{EXPECTED_SKIP}'")
    assert df.count() == 0, "unforced skip <> 0"
