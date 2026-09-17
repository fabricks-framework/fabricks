"""Real DAG ordering, forced-failure/skip bookkeeping, real fabricks.last_schedule state.

The runtime's tag="test" schedule itself runs once per session via
conftest.py's autouse _schedule_run fixture (the same generate -> process
(per step, real dbutils.notebook.run) -> terminate sequence
fabricks_run_job's tasks in databricks.yml drive) -- every tagged job runs
there, in parallel where the DAG allows, rather than one at a time via a
direct get_job(...).run() in an individual test. This module just asserts
against the resulting fabricks.last_schedule catalog state.

One test per runtime job/feature -- each tagged job gets exactly one
assertion naming what it proves, rather than one test per SQL query shape.
"""

from fabricks.context import SPARK
from fabricks.core import get_job

EXPECTED_FAILURES = {
    "gold.check_duplicate_key",
    "gold.check_fail",
    "gold.check_max_rows",
    "gold.invoke_failed_pre_run",
    "gold.invoke_timeout",
}
EXPECTED_SKIP = "gold.check_skip"
EXPECTED_WARNING = "gold.check_warning"


def _row(job: str):
    return SPARK.sql(f"select * from fabricks.last_schedule where job = '{job}'").collect()[0]


def _succeeded(job: str) -> bool:
    row = _row(job)
    return not row.failed and not row.skipped


def test_bronze_register_mode_king():
    # bronze.king_scd1: external-table registration against its seeded Delta
    # table -- the dummy-parser plugin itself is proven separately by
    # bronze.feature_parser below.
    assert _succeeded("bronze.king_scd1")


def test_bronze_register_mode_queen():
    assert _succeeded("bronze.queen_scd1")


def test_bronze_feature_parser():
    # bronze.feature_parser: real file parsing via the "dummy" custom parser
    # plugin (fabricks/parsers/dummy.py) -- register mode (king/queen above)
    # never calls get_parser(), so this is the only place that proves plugin
    # loading works. test_feature.py's checkpoint-idempotency test does a
    # second, direct get_job(...).run() after the schedule -- this just
    # confirms the schedule's own (first) run succeeded.
    assert _succeeded("bronze.feature_parser")


def test_cross_layer_dependency():
    # silver.king_scd1 (parents: [bronze.king_scd1]) only runs after its parent.
    assert _row("bronze.king_scd1").end_time < _row("silver.king_scd1").start_time


def test_silver_queen_scd1():
    assert _succeeded("silver.queen_scd1")


def test_silver_feature_parser():
    assert _succeeded("silver.feature_parser")


def test_auto_detected_gold_dependency():
    # gold.fact_dependency's SQL joins gold.dim_time + silver.king_scd1__current;
    # both parents are auto-detected via sqlglot, no wait_for needed.
    assert _row("gold.dim_time").end_time < _row("gold.fact_dependency").start_time
    assert _row("silver.king_scd1").end_time < _row("gold.fact_dependency").start_time


def test_feature_wait_for_dependency():
    # gold.feature_wait_for: explicit wait_for=[gold.dim_time], no notebook
    # invocation involved -- isolates plain dependency ordering from
    # notebook-mode's dbutils.notebook.run handoff (gold.dependency_notebook).
    assert _succeeded("gold.feature_wait_for")
    assert _row("gold.dim_time").end_time < _row("gold.feature_wait_for").start_time


def test_gold_dependency_notebook():
    # gold.dependency_notebook: notebook-derived dependency, schema inferred
    # via a real dbutils.notebook.run child-notebook invocation -- see
    # test_dependencies.py for the separate persisted-dependency-graph check.
    assert _succeeded("gold.dependency_notebook")


def test_gold_feature_extender():
    # gold.feature_extender: job-level extender_options applies the "dummy"
    # extender (fabricks/extenders/dummy.py) via Invoker.extend_job().
    assert _succeeded("gold.feature_extender")
    rows = SPARK.sql("select distinct extended_by from gold.feature_extender").collect()
    assert [r["extended_by"] for r in rows] == ["dummy"]


def test_gold_feature_udf():
    # gold.feature_udf: udf_dummy (fabricks/udfs/dummy.sql) registered via
    # register_all_udfs() and called directly in the job's SQL.
    assert _succeeded("gold.feature_udf")
    rows = SPARK.sql("select dummy from gold.feature_udf order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["dummy_1", "dummy_2"]


def test_gold_feature_mask():
    # gold.feature_mask: table_options.masks.dummy applies mask_dummy
    # (fabricks/masks/dummy.sql) -- unconditionally (no caller-identity
    # check), so the real masked value is visible to any query, proving the
    # mask function actually runs, not just that the DDL was issued.
    assert _succeeded("gold.feature_mask")
    rows = SPARK.sql("select dummy from gold.feature_mask order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["***", "2"]


def test_gold_feature_cluster_by():
    # gold.feature_cluster_by: table_options.cluster_by enables real liquid
    # clustering -- Table.liquid_clustering_enabled reads the Delta table
    # feature back to confirm it actually took effect, not just that the
    # option was set (that DDL-shape half is already covered by
    # unit/config/test_create_table_defaults.py).
    assert _succeeded("gold.feature_cluster_by")
    job = get_job(step="gold", topic="feature", item="cluster_by")
    assert job.table.liquid_clustering_enabled


def test_gold_type_widening_overwrite():
    # gold.type_widening_overwrite: schedule provides the (int) first write;
    # test_feature.py's follow-up test feeds a widened (double) batch after
    # and checks the physical column type actually changed.
    assert _succeeded("gold.type_widening_overwrite")


def test_gold_type_widening_merge():
    assert _succeeded("gold.type_widening_merge")


def test_gold_invoke_notebook():
    assert _succeeded("gold.invoke_notebook")


def test_gold_invoke_post_run():
    assert _succeeded("gold.invoke_post_run")


def test_gold_invoke_failed_pre_run():
    # gold.invoke_failed_pre_run: invoker_options.pre_run notebook
    # deliberately raises -- proves pre-run invoker failure propagates as a
    # job failure (see EXPECTED_FAILURES).
    assert _row("gold.invoke_failed_pre_run").failed


def test_forced_failure():
    assert _row("gold.check_fail").failed


def test_failure_causes():
    assert _row("gold.check_fail").exception.message == "Please don't fail on me :("
    assert _row("gold.check_max_rows").exception.message == "max rows check failed (3 > 2)"
    assert _row("gold.check_duplicate_key").exception.message == "duplicate __key check failed (1)"
    timeout_message = _row("gold.invoke_timeout").exception.message.lower()
    assert "timed out" in timeout_message or "timeout" in timeout_message


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
    assert row.done
    assert not row.failed
    assert SPARK.sql(f"select count(*) from {EXPECTED_WARNING}").collect()[0][0] == 1


def test_custom_view():
    # fabricks.dummy (fabricks/views/dummy.sql): create_or_replace_views()
    # deploys custom views from PATH_VIEWS at armageddon time.
    df = SPARK.sql("select * from fabricks.dummy")
    assert df.count() > 0


def test_no_unforced_failures():
    rows = SPARK.sql("select job from fabricks.last_schedule where failed").collect()
    assert {row.job for row in rows} == EXPECTED_FAILURES


def test_no_unforced_skips():
    rows = SPARK.sql("select job from fabricks.last_schedule where skipped").collect()
    assert {row.job for row in rows} == {EXPECTED_SKIP}
