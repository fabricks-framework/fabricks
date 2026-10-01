"""Truncate (https://github.com/fabricks-framework/fabricks/issues/66) beyond the single SCD2 case in
test_cdc_truncate.py: SCD1, an empty target, a key re-opened afterwards, and exact validity windows."""

from fabricks.cdc import SCD1, SCD2
from tests.spark.apache.cdc_frames import FAR_FUTURE, batch, history, rows

KEYS = {"keys": "id", "add_key": True}
D1, D2, D3, D5 = (f"2022-01-0{d} 00:00:00" for d in (1, 2, 3, 5))


def test_scd1_truncate_with_soft_delete_flags_every_row_and_adds_none(local_spark):
    scd1 = SCD1("cdc", "trunc_scd1", "case", spark=local_spark)
    options = {**KEYS, "soft_delete": True}
    scd1.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1)), **options)

    scd1.update(batch(local_spark, (None, None, "truncate", D2)), **options)

    assert rows(scd1.table, "id", "name", "__is_current", "__is_deleted") == [
        (1, "a", False, True),
        (2, "b", False, True),
    ]


def test_scd2_truncate_on_an_empty_target_adds_no_rows(local_spark):
    scd2 = SCD2("cdc", "trunc_empty", "case", spark=local_spark)

    scd2.update(batch(local_spark, (None, None, "truncate", D1)), **KEYS)

    assert rows(scd2.table, "id", "name") == []


def test_scd2_truncate_closes_with_the_exact_validity_window(local_spark):
    scd2 = SCD2("cdc", "trunc_window", "case", spark=local_spark)
    scd2.update(batch(local_spark, (1, "v1", "upsert", D1), (1, "v2", "upsert", D3)), **KEYS)

    scd2.update(batch(local_spark, (None, None, "truncate", D5)), **KEYS)

    assert history(scd2.table, 1) == [
        ("v1", D1, "2022-01-02 23:59:59", False),
        ("v2", D3, "2022-01-04 23:59:59", False),
    ]


def test_scd2_key_can_be_reopened_after_a_truncate(local_spark):
    scd2 = SCD2("cdc", "trunc_reopen", "case", spark=local_spark)
    scd2.update(batch(local_spark, (1, "a", "upsert", D1)), **KEYS)
    scd2.update(batch(local_spark, (None, None, "truncate", D3)), **KEYS)

    scd2.update(batch(local_spark, (1, "a", "upsert", D5)), **KEYS)

    assert history(scd2.table, 1) == [("a", D1, "2022-01-02 23:59:59", False), ("a", D5, FAR_FUTURE, True)]
