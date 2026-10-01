"""Focused SCD1 behaviours on real Spark/Delta: deletes and revival, out-of-order and repeated input, empty
batches, null values, composite keys, delete_missing and complete. Every test owns its table."""

import pytest

from fabricks.cdc import SCD1
from tests.spark.apache.cdc_frames import batch, empty_like, rows

KEYS = {"keys": "id", "add_key": True}
T1, T2, T3, T5 = ("2022-01-01 00:00:00", "2022-01-02 00:00:00", "2022-01-03 00:00:00", "2022-01-05 00:00:00")


def _make(spark, name: str) -> SCD1:
    return SCD1("cdc", f"scd1_{name}", "case", spark=spark)


def test_delete_operation_removes_the_row_when_soft_delete_is_off(local_spark):
    scd1 = _make(local_spark, "hard_delete")
    scd1.update(batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1)), **KEYS)

    scd1.update(batch(local_spark, (1, "a", "delete", T2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(2, "b")]


def test_soft_delete_marks_the_row_and_a_later_upsert_revives_it(local_spark):
    scd1 = _make(local_spark, "soft_revive")
    options = {**KEYS, "soft_delete": True}
    scd1.update(batch(local_spark, (1, "a", "upsert", T1)), **options)

    scd1.update(batch(local_spark, (1, "a", "delete", T2)), **options)
    assert rows(scd1.table, "id", "name", "__is_current", "__is_deleted") == [(1, "a", False, True)]

    scd1.update(batch(local_spark, (1, "a2", "upsert", T3)), **options)
    assert rows(scd1.table, "id", "name", "__is_current", "__is_deleted") == [(1, "a2", True, False)]


def test_an_older_timestamp_never_overwrites_a_newer_row(local_spark):
    scd1 = _make(local_spark, "late")
    scd1.update(batch(local_spark, (1, "new", "upsert", T5)), **KEYS)

    scd1.update(batch(local_spark, (1, "old", "upsert", T3)), **KEYS)

    assert [(r["name"], str(r["__timestamp"])) for r in scd1.table.dataframe.collect()] == [("new", T5)]


def test_rerunning_the_same_batch_changes_nothing(local_spark):
    scd1 = _make(local_spark, "idempotent")
    same = batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1))
    scd1.update(same, **KEYS)
    before = rows(scd1.table, "id", "name", "__timestamp", "__key")

    scd1.update(same, **KEYS)

    assert rows(scd1.table, "id", "name", "__timestamp", "__key") == before


def test_an_empty_batch_leaves_the_table_unchanged(local_spark):
    scd1 = _make(local_spark, "empty")
    first = batch(local_spark, (1, "a", "upsert", T1))
    scd1.update(first, **KEYS)

    scd1.update(empty_like(local_spark, first), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "a")]


def test_a_null_attribute_is_replaced_by_a_later_value(local_spark):
    scd1 = _make(local_spark, "null_attr")
    scd1.update(batch(local_spark, (1, None, "upsert", T1)), **KEYS)

    scd1.update(batch(local_spark, (1, "x", "upsert", T2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "x")]


def test_a_value_can_be_replaced_by_null(local_spark):
    scd1 = _make(local_spark, "to_null")
    scd1.update(batch(local_spark, (1, "x", "upsert", T1)), **KEYS)

    scd1.update(batch(local_spark, (1, None, "upsert", T2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, None)]


def test_composite_keys_update_only_the_matching_combination(local_spark):
    scd1 = _make(local_spark, "composite")
    columns = ["id", "region", "name", "__operation", "__timestamp"]
    options = {"keys": ["id", "region"], "add_key": True}
    scd1.update(
        batch(local_spark, (1, "eu", "a", "upsert", T1), (1, "us", "b", "upsert", T1), columns=columns), **options
    )

    scd1.update(batch(local_spark, (1, "eu", "a2", "upsert", T2), columns=columns), **options)

    assert rows(scd1.table, "id", "region", "name") == [(1, "eu", "a2"), (1, "us", "b")]


def test_delete_missing_hard_deletes_keys_absent_from_the_source(local_spark):
    scd1 = _make(local_spark, "dm_hard")
    scd1.update(batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1)), **KEYS)

    scd1.delete_missing(batch(local_spark, (1, "a", "upsert", T2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "a")]


def test_delete_missing_with_soft_delete_flags_absent_keys_and_keeps_present_ones(local_spark):
    scd1 = _make(local_spark, "dm_soft")
    options = {**KEYS, "soft_delete": True}
    scd1.update(batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1)), **options)

    scd1.delete_missing(batch(local_spark, (1, "a", "upsert", T2)), **options)

    assert rows(scd1.table, "id", "name", "__is_current", "__is_deleted") == [
        (1, "a", True, False),
        (2, "b", False, True),
    ]


def test_delete_missing_with_an_empty_source_deletes_every_current_row(local_spark):
    scd1 = _make(local_spark, "dm_empty")
    options = {**KEYS, "soft_delete": True}
    first = batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1))
    scd1.update(first, **options)

    scd1.delete_missing(empty_like(local_spark, first), **options)

    assert rows(scd1.table, "id", "__is_current", "__is_deleted") == [(1, False, True), (2, False, True)]


def test_complete_replaces_the_whole_table(local_spark):
    scd1 = _make(local_spark, "complete")
    scd1.complete(batch(local_spark, (1, "a", "upsert", T1), (2, "b", "upsert", T1)), **KEYS)

    scd1.complete(batch(local_spark, (2, "b2", "upsert", T2)), **KEYS)

    assert rows(scd1.table, "id", "name") == [(2, "b2")]


@pytest.mark.parametrize("soft_delete", [True, False], ids=["soft", "hard"])
def test_deleting_an_unknown_key_creates_nothing(local_spark, soft_delete):
    scd1 = _make(local_spark, f"delete_unknown_{soft_delete}")
    scd1.update(batch(local_spark, (1, "a", "upsert", T1)), soft_delete=soft_delete, **KEYS)

    scd1.update(batch(local_spark, (9, "ghost", "delete", T2)), soft_delete=soft_delete, **KEYS)

    assert rows(scd1.table, "id", "name") == [(1, "a")]
