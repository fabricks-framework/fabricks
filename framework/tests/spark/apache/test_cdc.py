import os
from pathlib import Path

import pytest

from fabricks.cdc import SCD1, NoCDC
from fabricks.cdc.scd0 import SCD0
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


# order_duplicate_by/deduplicate are **kwargs read directly by
# Processor.get_query_context (fabricks/cdc/base/processor.py) -- available
# on every CDC class's merge()/complete()/update() call, not a Gold-job-only
# concept, so no get_job()/YAML config is needed to exercise them. Must use
# SCD1 here, not NoCDC: dedup_key/dedup_hash only skip the `qualify` clause
# (fabricks/cdc/templates/ctes/deduplicate_key.sql.jinja's "advanced_ctes"
# branch, plain row_number()+filter instead) for change_data_capture in
# ["scd0","scd1","scd2"] -- NoCDC always takes the `qualify` branch, which
# OSS Spark's parser rejects (Databricks' does not; fix_sql only transpiles
# to the "databricks" sqlglot dialect, it doesn't rewrite qualify away).
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


# Processor.get_query_context (fabricks/cdc/base/processor.py) only strips
# unrecognized `__`-prefixed input columns from `outputs` for scd0/scd1/scd2
# (`fields`, i.e. non-`__` columns, plus the specific system columns each
# mode explicitly re-adds) -- NoCDC's outputs = inputs verbatim, so this is
# an SCD-only guarantee, not something every CDC class provides. Mirrors the
# old suite's gold-memory-mode test's intent (an internal working column
# from upstream must never leak into a job's real output), proven directly
# at the CDC-class level since the mechanism doesn't depend on job mode.
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


# (seed_from, iters, compare_to): king_and_queen_built seeds the target table
# from iteration `seed_from`'s *expected* output (seed_from=0 -> no seed, the
# from-empty-table bulk-load path), then runs one real SCD1/SCD2.update()
# call per iteration number in `iters`, in order, against that same table --
# each call's output feeding the next, with no reseeding in between.
# Comparison is always against iteration `compare_to`'s expected state.
#
# Most entries below are single-hop (seed from N-1's expected, run
# iteration N once) -- independent, seeded-from-expected steps that can run
# in any order, individually, or in parallel, see king_and_queen_built's
# docstring in conftest.py for why that's safe. The last two are deliberate
# multi-batch scenarios: chaining several real merges back-to-back (no
# oracle reseed between them) to check that a run of consecutive batches
# lands on the same state a fully independent, single-hop test would. Kept
# to a couple of scenarios, not every iteration, to avoid the O(n^2)
# full-chain replay this fixture was redesigned away from.
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
    # iter10: queen has only a delete op (nothing to delete against — a
    # no-op batch for queen). iter11: queen has no data at all this
    # iteration — king_and_queen_built skips it entirely, leaving queen's
    # rows from iter10 untouched in the target table. Both still have real
    # oracle SQL (tests/spark/expected/{scd1,scd2}/iter{10,11}.sql),
    # same as 1-9.
    (9, [10], 10),
    (10, [11], 11),
    (3, [4, 5, 6, 7], 7),
    (0, [1, 2, 3, 4, 5, 6, 7, 8, 9], 9),
]


def _assert_scenario_consistent(seed_from, iters, compare_to):
    assert iters == list(range(seed_from + 1, iters[-1] + 1)), (
        "iters must be the contiguous range right after seed_from"
    )
    assert iters[-1] == compare_to, "compare_to must be iters' own last element"


@pytest.mark.order(10)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_scd2_update(local_spark, king_and_queen_built, seed_from, iters, compare_to):
    _assert_scenario_consistent(seed_from, iters, compare_to)
    scd2 = king_and_queen_built(seed_from, iters, "scd2")
    compare_to_expected(local_spark, table=scd2.table, cdc="scd2", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(11)
@pytest.mark.parametrize(("seed_from", "iters", "compare_to"), _SCENARIOS)
def test_scd1_update(local_spark, king_and_queen_built, seed_from, iters, compare_to):
    _assert_scenario_consistent(seed_from, iters, compare_to)
    scd1 = king_and_queen_built(seed_from, iters, "scd1")
    compare_to_expected(local_spark, table=scd1.table, cdc="scd1", iter=compare_to, topic="king_and_queen")


# king_and_queen_built always passes correct_valid_from=True for scd2 -- the
# from-scratch batch (seed_from=0) is the one case where a row's real earliest
# __valid_from is also the batch's global min, so scd2.sql.jinja's
# __correct_valid_from CTE replaces it with the 1900-01-01 sentinel (see
# fabricks/cdc/templates/queries/scd2.sql.jinja). compare_to_expected already
# proves the whole table matches row-for-row; this asserts the specific
# behavior directly instead of leaving it implicit in that comparison.
@pytest.mark.order(13)
def test_scd2_correct_valid_from(local_spark, king_and_queen_built):
    scd2 = king_and_queen_built(0, [1], "scd2")
    min_valid_from = scd2.table.dataframe.selectExpr(
        "date_format(min(__valid_from), 'yyyy-MM-dd HH:mm:ss') as m"
    ).collect()[0]["m"]
    assert min_valid_from == "1900-01-01 00:00:00", "min __valid_from should be corrected to the sentinel"


# SCD0's merge template (fabricks/cdc/templates/merges/scd0.sql.jinja) has
# only a `when not matched ... insert` clause, no `when matched` at all, so a
# key already in the target is never touched again no matter what a later
# run's source snapshot says. Which database the target table lives in
# doesn't change that -- proven here the same way scd1/scd2 are above, no
# separate rename/duplicate oracle needed (see create_expected_views'
# "scd0" branch).
#
# Two real snapshots of king_and_queen's own already-proven SCD1 __current
# state (iteration 1, then iterations 1+2) feed two sequential SCD0.update()
# calls on the same target -- mirroring two real production runs -- proving
# new keys get inserted and existing keys' values stay frozen even though
# iteration 2 changes some of them.
@pytest.mark.order(12)
def test_scd0(local_spark, king_and_queen_built):
    create_expected_views(local_spark, "scd0")

    scd0 = SCD0("cdc", "scd0_test", "update", spark=local_spark)
    for last_iter in (1, 2):
        scd1 = king_and_queen_built(0, list(range(1, last_iter + 1)), "scd1")
        source = scd1.table.dataframe.where("__is_current and not __is_deleted").selectExpr(
            "id as __key", "id", "name", "doubleField"
        )
        scd0.update(source)
        compare_to_expected(local_spark, table=scd0.table, cdc="scd0", iter=last_iter, topic="king_and_queen")
