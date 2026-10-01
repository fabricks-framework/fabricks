"""Multi-source `mode: update` against real Spark and Delta, SCD1 and SCD2.

Invariant: `fix_context()` filters each source to rows newer than that source's own merged max timestamp, and a
source the target has never seen is loaded in full. SCD1 is also the real-Spark counterpart of
tests/unit/config/test_fix_context_avoids_full_hash_scan.py
(https://github.com/fabricks-framework/fabricks/issues/184). Narrow on purpose: the merge after the probe
legitimately hashes every field.
"""

from pyspark.sql.types import Row

from fabricks.cdc import SCD1, SCD2


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


def test_scd2_update_has_source_applies_per_source_watermark_and_loads_a_new_source(local_spark):
    cdc = SCD2("cdc", "multi_source_scd2", spark=local_spark)

    # the sources sit at different watermarks (king 01-05, queen 01-01): a single global watermark would also
    # discard the queen's genuinely new rows
    first = [
        Row(id=1, name="a", __source="king", __timestamp="2024-01-05 00:00:00"),
        Row(id=2, name="b", __source="queen", __timestamp="2024-01-01 00:00:00"),
    ]
    cdc.update(local_spark.createDataFrame(first), keys="id", add_key=True)

    second = [
        Row(id=1, name="a2", __source="king", __timestamp="2024-01-07 00:00:00"),  # new version, newer than king
        Row(id=2, name="b2", __source="queen", __timestamp="2024-01-04 00:00:00"),  # new version, older than king
        Row(id=3, name="c", __source="king", __timestamp="2024-01-06 00:00:00"),  # new key, newer than king
        Row(id=4, name="d", __source="queen", __timestamp="2024-01-03 00:00:00"),  # new key, older than king
        Row(id=5, name="e", __source="king", __timestamp="2024-01-03 00:00:00"),  # older than the king watermark
        Row(id=6, name="f", __source="prince", __timestamp="2024-01-02 00:00:00"),  # source not in the target yet
    ]
    cdc.update(local_spark.createDataFrame(second), keys="id", add_key=True)

    fmt = "yyyy-MM-dd HH:mm:ss"
    result = (
        cdc.table.dataframe.selectExpr(
            "id",
            "name",
            "__source as source",
            f"date_format(__valid_from, '{fmt}') as valid_from",
            f"date_format(__valid_to, '{fmt}') as valid_to",
            "__is_current as is_current",
        )
        .orderBy("id", "valid_from")
        .collect()
    )
    forever = "9999-12-31 00:00:00"
    assert [(r.id, r.name, r.source, r.valid_from, r.valid_to, r.is_current) for r in result] == [
        (1, "a", "king", "2024-01-05 00:00:00", "2024-01-06 23:59:59", False),
        (1, "a2", "king", "2024-01-07 00:00:00", forever, True),
        (2, "b", "queen", "2024-01-01 00:00:00", "2024-01-03 23:59:59", False),
        (2, "b2", "queen", "2024-01-04 00:00:00", forever, True),
        (3, "c", "king", "2024-01-06 00:00:00", forever, True),
        (4, "d", "queen", "2024-01-03 00:00:00", forever, True),
        (6, "f", "prince", "2024-01-02 00:00:00", forever, True),
    ], "per-source watermark: id 5 is older than the king's; ids 2 and 4 are older than it but newer than the queen's"
