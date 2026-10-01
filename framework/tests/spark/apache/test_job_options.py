"""Real get_job()-driven job behaviors that don't fit test_cdc.py (CDC-class level) or test_feature.py (DDL level).

Jobs are driven directly through BaseJob.for_each_batch(), not a scheduled run.
"""

import pytest

from tests.spark.expected.compare import compare_to_expected, create_expected_views
from tests.support.fixture_data import load_combined_frame


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


def test_append_mode_accumulates_across_batches(local_spark, fresh_job):
    job = fresh_job("silver", "append_test", "test")

    job.for_each_batch(local_spark.createDataFrame([(1, "a")], ["id", "name"]))
    assert job.table.dataframe.count() == 1

    job.for_each_batch(local_spark.createDataFrame([(2, "b")], ["id", "name"]))
    assert job.table.dataframe.count() == 2


# The job sets deduplicate:false because NoCDC's qualify-based dedup CTE is rejected by OSS Spark's parser
# (see test_cdc.py); safe here as neither batch has duplicate keys.
def test_latest_mode_replaces_target_with_each_batchs_full_snapshot(local_spark, fresh_job):
    job = fresh_job("silver", "latest_test", "test")
    columns = ["id", "name", "__operation", "__timestamp"]

    job.for_each_batch(local_spark.createDataFrame([(1, "a", "reload", "2022-01-01 00:00:00")], columns))
    assert job.table.dataframe.count() == 1

    job.for_each_batch(
        local_spark.createDataFrame(
            [(1, "a", "reload", "2022-01-02 00:00:00"), (2, "b", "reload", "2022-01-02 00:00:00")], columns
        )
    )
    assert job.table.dataframe.count() == 2


# https://github.com/fabricks-framework/fabricks/issues/182: an empty "latest" batch raised PARSE_SYNTAX_ERROR.
# The fixture's check_options.min_rows:0 stops batch_has_data() returning early, before the CDC layer.
def test_latest_mode_accepts_an_empty_batch(local_spark, fresh_job):
    job = fresh_job("silver", "latest_test", "test")
    columns = ["id", "name", "__operation", "__timestamp"]

    batch = local_spark.createDataFrame([(1, "a", "reload", "2022-01-01 00:00:00")], columns)
    job.for_each_batch(batch)
    assert job.table.dataframe.count() == 1

    job.for_each_batch(local_spark.createDataFrame([], schema=batch.schema))
    assert job.table.dataframe.count() == 0


def test_silver_scd1_handles_incremental_schema_drift(local_spark, cdc_oracles, fresh_job):
    job = fresh_job("silver", "king_and_queen", "scd1")

    job.for_each_batch(_iteration(local_spark, 1))
    job.update_schema(_iteration(local_spark, 2))
    job.for_each_batch(_iteration(local_spark, 2))

    compare_to_expected(local_spark, table=job.table, cdc="scd1", iter=2, topic="king_and_queen")


def test_silver_scd2_first_load_wires_validity_options(local_spark, cdc_oracles, fresh_job):
    job = fresh_job("silver", "king_and_queen", "scd2")

    job.for_each_batch(_iteration(local_spark, 1))

    compare_to_expected(local_spark, table=job.table, cdc="scd2", iter=1, topic="king_and_queen")
