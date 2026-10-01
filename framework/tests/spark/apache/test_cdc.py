import os
from pathlib import Path

import pytest

from fabricks.cdc import SCD1, NoCDC
from fabricks.cdc.scd0 import SCD0
from tests.spark.apache.cdc_harness import run_cdc_scenario
from tests.spark.expected.compare import compare_to_expected, create_expected_views


def test_local_spark_uses_small_fixture_parallelism(local_spark):
    assert local_spark.sparkContext.defaultParallelism == 2
    assert local_spark.conf.get("spark.databricks.delta.snapshotPartitions") == "2"
    assert local_spark.conf.get("spark.databricks.delta.merge.repartitionBeforeWrite.enabled") == "false"


def test_expected_cache_is_outside_disposable_storage():
    assert not Path(os.environ["FABRICKS_TEST_EXPECTED_CACHE"]).is_relative_to(
        Path(os.environ["FABRICKS_TEST_DISPOSABLE_STORAGE"])
    )


@pytest.mark.order(1)
def test_nocdc_overwrite(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("cdc", "nocdc", "overwrite", spark=local_spark)

    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(2)
def test_nocdc_append(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("cdc", "nocdc", "append", spark=local_spark)

    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 2


# SCD1, not NoCDC: NoCDC always renders the `qualify` dedup branch, which OSS Spark's parser rejects (Databricks'
# does not); scd0/scd1/scd2 use a plain row_number() + filter instead.
@pytest.mark.order(3)
def test_order_duplicate_by(local_spark):
    df = local_spark.createDataFrame(
        [(1, 1, "upsert", "2022-01-01 00:00:00"), (1, 2, "upsert", "2022-01-01 00:00:00")],
        ["id", "dummy", "__operation", "__timestamp"],
    )
    scd1 = SCD1("cdc", "order_duplicate", "test", spark=local_spark)

    scd1.update(df, keys="id", add_key=True, order_duplicate_by={"dummy": "desc"})

    rows = scd1.table.dataframe.collect()
    assert len(rows) == 1
    assert rows[0]["dummy"] == 2


@pytest.mark.order(4)
def test_deduplicate(local_spark):
    df = local_spark.createDataFrame(
        [(1, 1, "upsert", "2022-01-01 00:00:00"), (1, 1, "upsert", "2022-01-01 00:00:00")],
        ["id", "dummy", "__operation", "__timestamp"],
    )
    scd1 = SCD1("cdc", "deduplicate", "test", spark=local_spark)

    scd1.update(df, keys="id", add_key=True, deduplicate=True)

    assert scd1.table.dataframe.count() == 1


# Only scd0/scd1/scd2 strip unrecognized `__` input columns from the output; NoCDC passes its inputs through verbatim.
@pytest.mark.order(5)
def test_scd_output_drops_unrecognized_internal_columns(local_spark):
    df = local_spark.createDataFrame(
        [(1, "a", "upsert", "2022-01-01 00:00:00", "should not survive")],
        ["id", "name", "__operation", "__timestamp", "__should_not_be_found"],
    )
    scd1 = SCD1("cdc", "internal_column", "test", spark=local_spark)

    scd1.update(df, keys="id", add_key=True)

    assert "__should_not_be_found" not in scd1.table.dataframe.columns


@pytest.mark.order(6)
def test_special_char_columns_preserved(local_spark):
    df = local_spark.createDataFrame([(1, "a", 1.0)], ["@Id", "Näàme", "double Field!"])
    nocdc = NoCDC("cdc", "special_char", "test", spark=local_spark)

    nocdc.overwrite(df)

    assert set(nocdc.table.dataframe.columns) == {"@Id", "Näàme", "double Field!"}


@pytest.mark.order(7)
def test_delete_log_marks_rows_deleted(local_spark):
    scd1 = SCD1("cdc", "delete_log", "test", spark=local_spark)
    df1 = local_spark.createDataFrame(
        [(1, "a", "upsert", "2022-01-01 00:00:00")], ["id", "name", "__operation", "__timestamp"]
    )
    scd1.update(df1, keys="id", add_key=True, soft_delete=True)

    df2 = local_spark.createDataFrame(
        [(1, "a", "delete", "2022-01-02 00:00:00")], ["id", "name", "__operation", "__timestamp"]
    )
    scd1.update(df2, keys="id", add_key=True, soft_delete=True)

    assert scd1.table.dataframe.where("__is_deleted").count() == 1


# (seed_from, iters, compare_to): seed the table from iteration seed_from's expected output (0 = empty table), run one
# update() per iteration in iters in order without reseeding, compare to iteration compare_to's expected state.
_SCENARIOS = [
    (0, [1], 1),
    (1, [2], 2),
    (2, [3], 3),
    (3, [4], 4),
    (4, [5], 5),
    (5, [6], 6),
    (6, [7], 7),
    (7, [8], 8),
    (8, [9], 9),
    # iter10: queen has only a delete (a no-op for queen); iter11: queen has no data, so the scenario skips her.
    (9, [10], 10),
    (10, [11], 11),
    (3, [4, 5, 6, 7], 7),
    (0, [1, 2, 3, 4, 5, 6, 7, 8, 9], 9),
]


@pytest.mark.order(10)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_scd2_update(local_spark, seed_from, iters, compare_to):
    scd2 = run_cdc_scenario(local_spark, seed_from, iters, "scd2")
    compare_to_expected(local_spark, table=scd2.table, cdc="scd2", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(11)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_scd1_update(local_spark, seed_from, iters, compare_to):
    scd1 = run_cdc_scenario(local_spark, seed_from, iters, "scd1")
    compare_to_expected(local_spark, table=scd1.table, cdc="scd1", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(12)
def test_cdc_scenario_rebuilds_after_its_table_is_dropped(local_spark):
    first = run_cdc_scenario(local_spark, 0, [1], "scd2")
    first.table.drop()

    second = run_cdc_scenario(local_spark, 0, [1], "scd2")

    compare_to_expected(local_spark, table=second.table, cdc="scd2", iter=1, topic="king_and_queen")


# From scratch (seed_from=0) with correct_valid_from=True, scd2 replaces the earliest __valid_from with the sentinel.
@pytest.mark.order(13)
def test_scd2_correct_valid_from(local_spark):
    scd2 = run_cdc_scenario(local_spark, 0, [1], "scd2")
    min_valid_from = scd2.table.dataframe.selectExpr(
        "date_format(min(__valid_from), 'yyyy-MM-dd HH:mm:ss') as m"
    ).collect()[0]["m"]
    assert min_valid_from == "1900-01-01 00:00:00", "min __valid_from should be corrected to the sentinel"


# SCD0 merge has no `when matched` clause: new keys are inserted, existing keys stay frozen.
@pytest.mark.order(13)
def test_scd0(local_spark):
    create_expected_views(local_spark, "scd0")

    scd0 = SCD0("cdc", "scd0_test", "update", spark=local_spark)
    for last_iter in (1, 2):
        scd1 = run_cdc_scenario(local_spark, 0, list(range(1, last_iter + 1)), "scd1")
        source = scd1.table.dataframe.where("__is_current and not __is_deleted").selectExpr(
            "id as __key", "id", "name", "doubleField"
        )
        scd0.update(source)
        compare_to_expected(local_spark, table=scd0.table, cdc="scd0", iter=last_iter, topic="king_and_queen")
