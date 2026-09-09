"""Real get_job()-driven job-orchestration behaviors that don't fit
test_cdc.py (CDC-class-level) or test_feature.py (DDL-level) -- one test per
feature, matching tests/spark/databricks/test_schedule.py's convention.
Each job here is called directly through BaseJob.for_each_batch()/similar
real methods (not a full scheduled run) since these behaviors don't depend
on bronze/dependency plumbing.
"""

import pytest

from fabricks.core import get_job
from tests.spark.expected.compare import compare_to_expected, create_expected_views
from tests.spark.test_data import load_combined_frame


@pytest.fixture(scope="session")
def cdc_oracles(local_spark):
    for cdc in ("scd2", "scd1"):
        if not local_spark.catalog.tableExists(f"expected.{cdc}_iter1"):
            create_expected_views(local_spark, cdc)


def _iteration(spark, number: int):
    frame = load_combined_frame(spark, number)
    # Silver consumes Bronze output, where the business key is already derived.
    return frame.selectExpr(
        "*", "md5(array_join(array(cast(id as string), cast(__source as string)), '*', '-1')) as __key"
    )


def test_append_mode_accumulates_across_batches(local_spark):
    job = get_job(step="silver", topic="append_test", item="test")

    job.for_each_batch(local_spark.createDataFrame([(1, "a")], ["id", "name"]))
    assert job.table.dataframe.count() == 1

    job.for_each_batch(local_spark.createDataFrame([(2, "b")], ["id", "name"]))
    assert job.table.dataframe.count() == 2


# "latest" mode's default (dedup on) always hits NoCDC's qualify-based dedup
# CTE (see test_cdc.py's test_order_duplicate_by/test_deduplicate comment),
# which OSS Spark's parser rejects but Databricks' does not -- this job sets
# the real, supported deduplicate:false option to sidestep that, valid here
# since neither batch below has duplicate keys within itself.
def test_latest_mode_replaces_target_with_each_batchs_full_snapshot(local_spark):
    job = get_job(step="silver", topic="latest_test", item="test")
    columns = ["id", "name", "__operation", "__timestamp"]

    job.for_each_batch(local_spark.createDataFrame([(1, "a", "reload", "2022-01-01 00:00:00")], columns))
    assert job.table.dataframe.count() == 1

    # "latest" mode is a full replace (NoCDC.complete() -> insert overwrite),
    # not an incremental merge -- it's up to the caller to feed a complete
    # current-state snapshot each batch, same as a real parser would.
    job.for_each_batch(
        local_spark.createDataFrame(
            [(1, "a", "reload", "2022-01-02 00:00:00"), (2, "b", "reload", "2022-01-02 00:00:00")], columns
        )
    )
    assert job.table.dataframe.count() == 2


def test_silver_scd1_handles_incremental_schema_drift(local_spark, cdc_oracles):
    job = get_job(step="silver", topic="king_and_queen", item="scd1")

    job.for_each_batch(_iteration(local_spark, 1))
    job.update_schema(_iteration(local_spark, 2))
    job.for_each_batch(_iteration(local_spark, 2))

    compare_to_expected(local_spark, table=job.table, cdc="scd1", iter=2, topic="king_and_queen")


def test_silver_scd2_first_load_wires_validity_options(local_spark, cdc_oracles):
    job = get_job(step="silver", topic="king_and_queen", item="scd2")

    job.for_each_batch(_iteration(local_spark, 1))

    compare_to_expected(local_spark, table=job.table, cdc="scd2", iter=1, topic="king_and_queen")
