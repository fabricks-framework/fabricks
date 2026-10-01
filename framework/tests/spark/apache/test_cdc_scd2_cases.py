"""Focused SCD2 behaviours on real Spark/Delta: multi-version history, deletes and revival, repeated input, empty
batches, nulls, composite keys, complete, delete_missing and correct_valid_from. Every test owns its table, and
every history test uses three or more strictly increasing versions so first/last and open/closed cannot be
confused."""

import pytest

from fabricks.cdc import SCD2
from tests.spark.apache.cdc_frames import FAR_FUTURE, batch, empty_like, history, rows

KEYS = {"keys": "id", "add_key": True}
D1, D2, D3, D5, D7 = (f"2022-01-0{d} 00:00:00" for d in (1, 2, 3, 5, 7))


def _make(spark, name: str) -> SCD2:
    return SCD2("cdc", f"scd2_{name}", "case", spark=spark)


def test_three_versions_in_separate_batches_form_a_closed_chain(local_spark):
    scd2 = _make(local_spark, "chain_batches")
    scd2.update(batch(local_spark, (1, "v1", "upsert", D1)), **KEYS)
    scd2.update(batch(local_spark, (1, "v2", "upsert", D3)), **KEYS)

    scd2.update(batch(local_spark, (1, "v3", "upsert", D5)), **KEYS)

    assert history(scd2.table, 1) == [
        ("v1", D1, "2022-01-02 23:59:59", False),
        ("v2", D3, "2022-01-04 23:59:59", False),
        ("v3", D5, FAR_FUTURE, True),
    ]


def test_three_versions_in_one_batch_form_the_same_chain(local_spark):
    scd2 = _make(local_spark, "chain_one_batch")

    scd2.update(batch(local_spark, (1, "v1", "upsert", D1), (1, "v2", "upsert", D3), (1, "v3", "upsert", D5)), **KEYS)

    assert history(scd2.table, 1) == [
        ("v1", D1, "2022-01-02 23:59:59", False),
        ("v2", D3, "2022-01-04 23:59:59", False),
        ("v3", D5, FAR_FUTURE, True),
    ]


def test_interleaved_keys_keep_independent_chains(local_spark):
    scd2 = _make(local_spark, "interleaved")
    scd2.update(
        batch(
            local_spark,
            (1, "a1", "upsert", D1),
            (2, "b1", "upsert", D3),
            (1, "a2", "upsert", D5),
            (2, "b2", "upsert", D7),
        ),
        **KEYS,
    )

    assert history(scd2.table, 1) == [("a1", D1, "2022-01-04 23:59:59", False), ("a2", D5, FAR_FUTURE, True)]
    assert history(scd2.table, 2) == [("b1", D3, "2022-01-06 23:59:59", False), ("b2", D7, FAR_FUTURE, True)]


def test_rerunning_the_same_batch_changes_nothing(local_spark):
    scd2 = _make(local_spark, "idempotent")
    same = batch(local_spark, (1, "v1", "upsert", D1), (2, "w1", "upsert", D1))
    scd2.update(same, **KEYS)
    before = history(scd2.table, 1), history(scd2.table, 2)

    scd2.update(same, **KEYS)

    assert (history(scd2.table, 1), history(scd2.table, 2)) == before
    assert scd2.table.dataframe.count() == 2


def test_an_empty_batch_leaves_the_table_unchanged(local_spark):
    scd2 = _make(local_spark, "empty")
    first = batch(local_spark, (1, "v1", "upsert", D1))
    scd2.update(first, **KEYS)

    scd2.update(empty_like(local_spark, first), **KEYS)

    assert history(scd2.table, 1) == [("v1", D1, FAR_FUTURE, True)]


def test_a_newer_timestamp_with_unchanged_values_opens_no_new_version(local_spark):
    scd2 = _make(local_spark, "no_change")
    scd2.update(batch(local_spark, (1, "same", "upsert", D1)), **KEYS)

    scd2.update(batch(local_spark, (1, "same", "upsert", D3)), **KEYS)

    assert history(scd2.table, 1) == [("same", D1, FAR_FUTURE, True)]


@pytest.mark.parametrize(
    ("first", "second"), [pytest.param(None, "x", id="null-to-value"), pytest.param("x", None, id="value-to-null")]
)
def test_a_change_to_or_from_null_opens_a_new_version(local_spark, first, second):
    scd2 = _make(local_spark, f"null_{first}_{second}")
    scd2.update(batch(local_spark, (1, first, "upsert", D1)), **KEYS)

    scd2.update(batch(local_spark, (1, second, "upsert", D3)), **KEYS)

    assert history(scd2.table, 1) == [(first, D1, "2022-01-02 23:59:59", False), (second, D3, FAR_FUTURE, True)]


def test_soft_delete_closes_the_version_and_a_later_upsert_opens_a_new_one(local_spark):
    scd2 = _make(local_spark, "soft_revive")
    options = {**KEYS, "soft_delete": True}
    scd2.update(batch(local_spark, (1, "v1", "upsert", D1)), **options)

    scd2.update(batch(local_spark, (1, "v1", "delete", D2)), **options)
    assert rows(scd2.table, "name", "__is_current", "__is_deleted") == [("v1", False, True)]

    scd2.update(batch(local_spark, (1, "v2", "upsert", D5)), **options)
    assert history(scd2.table, 1) == [("v1", D1, "2022-01-01 23:59:59", False), ("v2", D5, FAR_FUTURE, True)]
    assert rows(scd2.table, "name", "__is_deleted") == [("v1", True), ("v2", False)]


def test_delete_without_soft_delete_closes_the_current_version(local_spark):
    scd2 = _make(local_spark, "hard_delete")
    scd2.update(batch(local_spark, (1, "v1", "upsert", D1)), **KEYS)

    scd2.update(batch(local_spark, (1, "v1", "delete", D2)), **KEYS)

    assert history(scd2.table, 1) == [("v1", D1, "2022-01-01 23:59:59", False)]


def test_delete_missing_with_soft_delete_closes_only_the_absent_keys(local_spark):
    scd2 = _make(local_spark, "dm_soft")
    options = {**KEYS, "soft_delete": True}
    scd2.update(batch(local_spark, (1, "v1", "upsert", D1), (2, "w1", "upsert", D1)), **options)

    scd2.delete_missing(batch(local_spark, (1, "v1", "upsert", D2)), **options)

    assert history(scd2.table, 1) == [("v1", D1, FAR_FUTURE, True)]
    assert history(scd2.table, 2) == [("w1", D1, "2022-01-01 23:59:59", False)]
    assert rows(scd2.table, "id", "__is_deleted") == [(1, False), (2, True)]


def test_delete_missing_with_an_empty_source_closes_every_current_version(local_spark):
    scd2 = _make(local_spark, "dm_empty")
    options = {**KEYS, "soft_delete": True}
    first = batch(local_spark, (1, "v1", "upsert", D1), (2, "w1", "upsert", D1))
    scd2.update(first, **options)

    scd2.delete_missing(empty_like(local_spark, first), **options)

    assert rows(scd2.table, "id", "__is_current", "__is_deleted") == [(1, False, True), (2, False, True)]


def test_complete_replaces_the_table_and_drops_the_old_history(local_spark):
    scd2 = _make(local_spark, "complete")
    scd2.complete(batch(local_spark, (1, "v1", "upsert", D1), (2, "w1", "upsert", D1)), **KEYS)

    scd2.complete(batch(local_spark, (1, "v2", "upsert", D2)), **KEYS)

    assert history(scd2.table, 1) == [("v2", D2, FAR_FUTURE, True)]
    assert scd2.table.dataframe.where("id = 2").count() == 0


@pytest.mark.parametrize(
    ("correct_valid_from", "first_valid_from"),
    [pytest.param(True, "1900-01-01 00:00:00", id="corrected"), pytest.param(False, D5, id="as-delivered")],
)
def test_correct_valid_from_only_moves_the_first_version_back(local_spark, correct_valid_from, first_valid_from):
    scd2 = _make(local_spark, f"cvf_{correct_valid_from}")

    scd2.update(
        batch(local_spark, (1, "v1", "upsert", D5), (1, "v2", "upsert", D7)),
        correct_valid_from=correct_valid_from,
        **KEYS,
    )

    assert history(scd2.table, 1) == [
        ("v1", first_valid_from, "2022-01-06 23:59:59", False),
        ("v2", D7, FAR_FUTURE, True),
    ]


def test_composite_keys_version_only_the_matching_combination(local_spark):
    scd2 = _make(local_spark, "composite")
    columns = ["id", "region", "name", "__operation", "__timestamp"]
    options = {"keys": ["id", "region"], "add_key": True}
    scd2.update(
        batch(local_spark, (1, "eu", "a", "upsert", D1), (1, "us", "b", "upsert", D1), columns=columns), **options
    )

    scd2.update(batch(local_spark, (1, "eu", "a2", "upsert", D3), columns=columns), **options)

    assert rows(scd2.table, "region", "name", "__is_current") == [
        ("eu", "a", False),
        ("eu", "a2", True),
        ("us", "b", True),
    ]
