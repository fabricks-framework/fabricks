"""NoCDC merge (update mode) executed on real Spark. NoCDC ignores `__operation`: a row changes only when its
key/hash differs, and rows leave the table only through `delete_missing`. The generated-SQL shape of this merge
is asserted in tests/unit/config/test_cdc_query_generation.py; this proves it runs."""

from fabricks.cdc import NoCDC
from tests.spark.apache.cdc_frames import batch, rows

KEYS = {"keys": "id", "add_key": True, "add_hash": True, "deduplicate": False}
D1, D2 = "2022-01-01 00:00:00", "2022-01-02 00:00:00"


def _nocdc(spark, name: str) -> NoCDC:
    return NoCDC("cdc", f"nocdc_merge_{name}", "case", spark=spark)


def test_update_mode_inserts_new_keys_and_updates_changed_ones(local_spark):
    nocdc = _nocdc(local_spark, "upsert")
    nocdc.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1)), **KEYS)

    nocdc.update(batch(local_spark, (1, "a2", "upsert", D2), (3, "c", "upsert", D2)), **KEYS)

    assert rows(nocdc.table, "id", "name") == [(1, "a2"), (2, "b"), (3, "c")]


def test_update_mode_does_not_delete_on_a_delete_operation(local_spark):
    nocdc = _nocdc(local_spark, "ignores_operation")
    nocdc.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1)), **KEYS)

    nocdc.update(batch(local_spark, (2, "b", "delete", D2)), **KEYS)

    assert rows(nocdc.table, "id", "name") == [(1, "a"), (2, "b")]


def test_delete_missing_removes_keys_absent_from_the_source(local_spark):
    nocdc = _nocdc(local_spark, "delete_missing")
    nocdc.update(batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1)), **KEYS)

    nocdc.delete_missing(batch(local_spark, (1, "a", "upsert", D2)), **KEYS)

    assert rows(nocdc.table, "id", "name") == [(1, "a")]


def test_update_mode_rerunning_a_batch_changes_nothing(local_spark):
    nocdc = _nocdc(local_spark, "idempotent")
    same = batch(local_spark, (1, "a", "upsert", D1), (2, "b", "upsert", D1))
    nocdc.update(same, **KEYS)

    nocdc.update(same, **KEYS)

    assert rows(nocdc.table, "id", "name") == [(1, "a"), (2, "b")]
