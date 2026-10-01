"""Real DAG ordering, forced-failure/skip bookkeeping, real fabricks.last_schedule state.

The tag="test" schedule runs once per session in conftest.py's _schedule_run fixture; each test asserts one
tagged job's outcome in the resulting fabricks.last_schedule state.
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
    assert _succeeded("bronze.king_scd1")


def test_bronze_register_mode_queen():
    assert _succeeded("bronze.queen_scd1")


def test_bronze_feature_parser():
    # Register mode (king/queen) never calls get_parser(), so only this job proves parser plugin loading.
    assert _succeeded("bronze.feature_parser")


def test_cross_layer_dependency():
    assert _row("bronze.king_scd1").end_time < _row("silver.king_scd1").start_time


def test_silver_queen_scd1():
    assert _succeeded("silver.queen_scd1")


def test_silver_feature_parser():
    assert _succeeded("silver.feature_parser")


def test_auto_detected_gold_dependency():
    # Both parents are auto-detected from the job's SQL (sqlglot); no wait_for.
    assert _row("gold.dim_time").end_time < _row("gold.fact_dependency").start_time
    assert _row("silver.king_scd1").end_time < _row("gold.fact_dependency").start_time


def test_gold_dependency_multi_parent_status_propagation():
    # DagProcessor._propagate_status() must mark every incoming edge ok, not just the first, before dispatch.
    assert _succeeded("gold.dependency_sql")
    for parent in ("gold.dim_time", "silver.king_scd1", "silver.queen_scd1", "transf.fact_memory"):
        assert _row(parent).end_time < _row("gold.dependency_sql").start_time


def test_feature_wait_for_dependency():
    # Explicit wait_for with no notebook: isolates plain ordering from dependency_notebook's notebook.run handoff.
    assert _succeeded("gold.feature_wait_for")
    assert _row("gold.dim_time").end_time < _row("gold.feature_wait_for").start_time


def test_gold_dependency_notebook():
    # Schema is inferred via a real dbutils.notebook.run child notebook (persisted graph: test_dependencies.py).
    assert _succeeded("gold.dependency_notebook")


def test_gold_feature_extender():
    assert _succeeded("gold.feature_extender")
    rows = SPARK.sql("select distinct extended_by from gold.feature_extender").collect()
    assert [r["extended_by"] for r in rows] == ["dummy"]


def test_gold_feature_udf():
    assert _succeeded("gold.feature_udf")
    rows = SPARK.sql("select dummy from gold.feature_udf order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["dummy_1", "dummy_2"]


def test_gold_feature_mask():
    # The mask applies unconditionally (no caller-identity check), so the masked value proves it ran, not just the DDL.
    assert _succeeded("gold.feature_mask")
    rows = SPARK.sql("select dummy from gold.feature_mask order by dummy").collect()
    assert [r["dummy"] for r in rows] == ["***", "2"]


def test_gold_feature_cluster_by():
    # Reads the Delta table feature back; the DDL shape is covered by unit/config/test_create_table_defaults.py.
    assert _succeeded("gold.feature_cluster_by")
    job = get_job(step="gold", topic="feature", item="cluster_by")
    assert job.table.liquid_clustering_enabled


def test_gold_type_widening_overwrite():
    # The schedule writes the int batch; test_feature.py then feeds a widened double batch.
    assert _succeeded("gold.type_widening_overwrite")


def test_gold_type_widening_merge():
    assert _succeeded("gold.type_widening_merge")


def test_gold_invoke_notebook():
    assert _succeeded("gold.invoke_notebook")


def test_gold_invoke_post_run():
    assert _succeeded("gold.invoke_post_run")


def test_gold_invoke_failed_pre_run():
    # The pre_run notebook raises on purpose; its failure must propagate as a job failure.
    assert _row("gold.invoke_failed_pre_run").failed


def test_forced_failure():
    assert _row("gold.check_fail").failed


def test_gold_check_zstd():
    # Spark Connect has no newSession(), so _derive_session (resolver.py) falls back to the parent (issue #215).
    # Asserts on the parquet file name (Delta embeds the codec) rather than the session conf, to prove it was applied.
    assert _succeeded("gold.check_zstd")
    job = get_job(step="gold", topic="check", item="zstd")
    file_names = [
        str(f["name"])
        for f in job.table.delta_path.get_file_info()
        if int(f["size"]) > 0 and str(f["name"]).endswith(".parquet")
    ]
    assert file_names, "no data files written"
    assert all("zstd" in name for name in file_names), "codec <> zstd"


def test_failure_causes():
    assert _row("gold.check_fail").exception.message == "Please don't fail on me :("
    assert _row("gold.check_max_rows").exception.message == "max rows check failed (3 > 2)"
    assert _row("gold.check_duplicate_key").exception.message == "duplicate __key check failed (1)"
    timeout_message = _row("gold.invoke_timeout").exception.message.lower()
    assert "timed out" in timeout_message or "timeout" in timeout_message


def test_forced_skip():
    assert _row(EXPECTED_SKIP).skipped


def test_forced_warning():
    # dags/run.py logs a CheckWarning as 'warned', not 'failed', and logs_pivot counts warned as done; the table is
    # still populated because Processor.run() raises the warning only at the very end.
    row = _row(EXPECTED_WARNING)
    assert row.warned
    assert row.done
    assert not row.failed
    assert SPARK.sql(f"select count(*) from {EXPECTED_WARNING}").collect()[0][0] == 1


def test_custom_view():
    df = SPARK.sql("select * from fabricks.dummy")
    assert df.count() > 0


def test_no_unforced_failures():
    rows = SPARK.sql("select job from fabricks.last_schedule where failed").collect()
    assert {row.job for row in rows} == EXPECTED_FAILURES


def test_no_unforced_skips():
    rows = SPARK.sql("select job from fabricks.last_schedule where skipped").collect()
    assert {row.job for row in rows} == {EXPECTED_SKIP}
