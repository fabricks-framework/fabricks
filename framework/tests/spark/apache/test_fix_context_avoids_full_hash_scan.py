"""Real-Spark counterpart of tests/unit/config/test_fix_context_avoids_full_hash_scan.py
(https://github.com/fabricks-framework/fabricks/issues/184).

Invariant: for a multi-source `mode: update`, `fix_context()` filters each source to rows newer than that
source's own merged max timestamp. Narrow on purpose: the merge after the probe legitimately hashes every field.
"""

from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def test_fix_context_update_probe_has_source_produces_correct_per_source_slice(local_spark):
    cdc = SCD1("cdc", "hash_scan_real", spark=local_spark)

    # the two sources sit at different watermarks: a single global watermark (the king's 01-05) would also
    # discard the queen's genuinely new row at 01-03
    first = [
        Row(id=1, name="a", __source="king", __timestamp="2024-01-05 00:00:00"),
        Row(id=2, name="b", __source="queen", __timestamp="2024-01-01 00:00:00"),
    ]
    cdc.update(local_spark.createDataFrame(first), keys="id", add_key=True)
    assert cdc.table.dataframe.count() == 2

    second = [
        Row(id=1, name="a", __source="king", __timestamp="2024-01-05 00:00:00"),  # already merged
        Row(id=3, name="c", __source="king", __timestamp="2024-01-06 00:00:00"),  # newer than the king watermark
        Row(
            id=4, name="d", __source="queen", __timestamp="2024-01-03 00:00:00"
        ),  # newer than the queen's, older than the king's
        Row(id=5, name="e", __source="king", __timestamp="2024-01-03 00:00:00"),  # older than the king watermark
    ]
    cdc.update(local_spark.createDataFrame(second), keys="id", add_key=True)

    result = cdc.table.dataframe.select("id", "name").orderBy("id").collect()
    assert [(r.id, r.name) for r in result] == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
