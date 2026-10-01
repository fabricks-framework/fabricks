"""CDC options that shape a batch before the merge (deduplication, ordering of duplicates, slicing, rectify) plus
the first-run and schema edge cases around them. Every test owns its table."""

import pytest

from fabricks.cdc import SCD1, SCD2
from tests.spark.apache.cdc_frames import FAR_FUTURE, batch, empty_like, history, rows

KEYS = {"keys": "id", "add_key": True}
D1, D2, D3, D5, D6 = (f"2022-01-0{d} 00:00:00" for d in (1, 2, 3, 5, 6))


def test_the_latest_row_of_a_key_wins_within_a_single_batch(local_spark):
    scd1 = SCD1("cdc", "opt_batch_latest", "case", spark=local_spark)

    scd1.update(batch(local_spark, (1, "a", "upsert", D1), (1, "b", "upsert", D2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "b")]


@pytest.mark.parametrize(
    ("direction", "expected"),
    [pytest.param("asc", [(1, "m"), (2, "c")], id="asc"), pytest.param("desc", [(1, "z"), (2, "y")], id="desc")],
)
def test_order_duplicate_by_breaks_timestamp_ties_per_key(local_spark, direction, expected):
    scd1 = SCD1("cdc", f"opt_odb_{direction}", "case", spark=local_spark)
    tied = batch(
        local_spark, (1, "m", "upsert", D1), (1, "z", "upsert", D1), (2, "y", "upsert", D1), (2, "c", "upsert", D1)
    )

    scd1.update(tied, order_duplicate_by={"name": direction}, **KEYS)

    assert rows(scd1.table, "id", "name") == expected


def test_slice_update_ignores_rows_older_than_the_merged_watermark_and_keeps_new_keys(local_spark):
    scd1 = SCD1("cdc", "opt_slice_update", "case", spark=local_spark)
    scd1.update(batch(local_spark, (1, "a", "upsert", D5)), **KEYS)

    scd1.update(batch(local_spark, (1, "old", "upsert", D3), (2, "new", "upsert", D6)), slice="update", **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "a"), (2, "new")]


def test_slice_latest_keeps_only_the_rows_at_the_newest_timestamp(local_spark):
    scd1 = SCD1("cdc", "opt_slice_latest", "case", spark=local_spark)

    scd1.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D2)), slice="latest", **KEYS)

    assert rows(scd1.table, "id", "name") == [(2, "b")]


def test_rectify_closes_keys_missing_from_a_reload_and_versions_changed_ones(local_spark):
    scd2 = SCD2("cdc", "opt_rectify", "case", spark=local_spark)
    scd2.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1), (3, "c", "upsert", D1)), **KEYS)

    scd2.update(batch(local_spark, (1, "a", "reload", D2), (2, "b2", "reload", D2)), rectify=True, **KEYS)

    assert history(scd2.table, 1) == [("a", D1, FAR_FUTURE, True)]
    assert history(scd2.table, 2) == [("b", D1, "2022-01-01 23:59:59", False), ("b2", D2, FAR_FUTURE, True)]
    assert history(scd2.table, 3) == [("c", D1, "2022-01-01 23:59:59", False)]


def test_a_batch_without_a_column_keeps_the_existing_values_of_that_column(local_spark):
    scd1 = SCD1("cdc", "opt_missing_column", "case", spark=local_spark)
    scd1.update(batch(local_spark, (1, "a", "upsert", D1)), **KEYS)

    scd1.update(batch(local_spark, (1, "upsert", D2), columns=["id", "__operation", "__timestamp"]), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "a")]


@pytest.mark.parametrize("cdc", [SCD1, SCD2], ids=["scd1", "scd2"])
def test_an_empty_first_batch_creates_an_empty_table(local_spark, cdc):
    target = cdc("cdc", f"opt_first_empty_{cdc.__name__}", "case", spark=local_spark)
    template = batch(local_spark, (1, "a", "upsert", D1))

    target.update(empty_like(local_spark, template), **KEYS)

    assert target.table.exists()
    assert rows(target.table, "id") == []


def test_add_key_false_keeps_the_key_the_caller_provided(local_spark):
    scd1 = SCD1("cdc", "opt_own_key", "case", spark=local_spark)
    own_key = local_spark.createDataFrame(
        [(1, "a", "upsert", D1, "k1")], "id int, name string, __operation string, __timestamp string, __key string"
    )

    scd1.update(own_key, keys="id", add_key=False)

    assert rows(scd1.table, "id", "__key") == [(1, "k1")]
