"""Multi-source `mode: update` against real Spark and Delta, SCD1 and SCD2.

Invariant: `fix_context()` filters each source to rows newer than that source's own merged max timestamp, and a
source the target has never seen is loaded in full. SCD1 is also the real-Spark counterpart of
tests/unit/config/test_fix_context_avoids_full_hash_scan.py
(https://github.com/fabricks-framework/fabricks/issues/184). Narrow on purpose: the merge after the probe
legitimately hashes every field.
"""

from pyspark.sql.types import Row
import pytest

from fabricks.cdc import SCD1, SCD2
from tests.spark.apache.cdc_frames import FAR_FUTURE, history, rows


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


D1, D3, D4 = (f"2022-01-0{d} 00:00:00" for d in (1, 3, 4))
SCHEMA = "id int, name string, __source string, __operation string, __timestamp string"


def _seed_and_reload_batch(spark, cdc):
    """The target holds king (1-3), queen (4-5) and prince (6-7). One batch then carries a king reload (plus a later
    king upsert of a reloaded key), plain queen upserts without a reload, and nothing at all for prince."""
    seed = [
        (1, "a", "king", "upsert", D1),
        (2, "b", "king", "upsert", D1),
        (3, "c", "king", "upsert", D1),
        (4, "d", "queen", "upsert", D1),
        (5, "e", "queen", "upsert", D1),
        (6, "f", "prince", "upsert", D1),
        (7, "g", "prince", "upsert", D1),
    ]
    cdc.update(spark.createDataFrame(seed, SCHEMA), keys="id", add_key=True)

    mixed = [
        (1, "a2", "king", "reload", D3),  # changed by the reload
        (3, "c", "king", "reload", D3),  # unchanged by the reload
        (8, "h", "king", "reload", D3),  # new in the reload; id 2 is missing from it
        (1, "a3", "king", "upsert", D4),  # upsert after the reload
        (4, "d2", "queen", "upsert", D3),  # changed; queen has no reload, so its id 5 is not in the batch
        (9, "i", "queen", "upsert", D3),  # new
    ]
    cdc.update(spark.createDataFrame(mixed, SCHEMA), keys="id", add_key=True)


def test_scd2_reload_of_one_source_does_not_close_the_keys_of_the_other_sources(local_spark):
    scd2 = SCD2("cdc", "multi_source_scd2_reload", spark=local_spark)

    _seed_and_reload_batch(local_spark, scd2)

    d3_end = "2022-01-02 23:59:59"
    d4_end = "2022-01-03 23:59:59"
    assert history(scd2.table, 1) == [
        ("a", D1, d3_end, False),
        ("a2", D3, d4_end, False),
        ("a3", D4, FAR_FUTURE, True),
    ]
    assert history(scd2.table, 2) == [("b", D1, d3_end, False)], "missing from the king reload: closed"
    assert history(scd2.table, 3) == [("c", D1, FAR_FUTURE, True)], "repeated by the king reload: no new version"
    assert history(scd2.table, 8) == [("h", D3, FAR_FUTURE, True)]
    assert history(scd2.table, 4) == [("d", D1, d3_end, False), ("d2", D3, FAR_FUTURE, True)]
    assert history(scd2.table, 5) == [("e", D1, FAR_FUTURE, True)], (
        "queen has no reload: absent from the batch is not deleted"
    )
    assert history(scd2.table, 9) == [("i", D3, FAR_FUTURE, True)]
    assert history(scd2.table, 6) == [("f", D1, FAR_FUTURE, True)], "prince has no rows in the batch: untouched"
    assert history(scd2.table, 7) == [("g", D1, FAR_FUTURE, True)], "prince has no rows in the batch: untouched"


def test_scd1_reload_of_one_source_does_not_delete_the_keys_of_the_other_sources(local_spark):
    scd1 = SCD1("cdc", "multi_source_scd1_reload", spark=local_spark)

    _seed_and_reload_batch(local_spark, scd1)

    assert rows(scd1.table, "id", "name", "__source") == [
        (1, "a3", "king"),
        (3, "c", "king"),
        (4, "d2", "queen"),
        (5, "e", "queen"),
        (6, "f", "prince"),
        (7, "g", "prince"),
        (8, "h", "king"),
        (9, "i", "queen"),
    ], "only the king's id 2 is missing from a reload; queen's id 5 and prince's rows must survive"


# What one source's batch does to its two seeded keys (base+1 "x1", base+2 "x2"), whatever the other sources do.
MODES = ("reload", "upsert", "delete", "none")
LIVE = {  # the (key offset -> name) pairs still current afterwards
    "reload": {1: "x1r", 3: "x3"},  # key 2 is missing from the reload
    "upsert": {1: "x1u", 2: "x2", 3: "x3"},
    "delete": {2: "x2"},
    "none": {1: "x1", 2: "x2"},
}


def _batch_rows(source, base, mode):
    def row(offset, name, operation):
        return (base + offset, name, source, operation, D3)

    return {
        "reload": [row(1, "x1r", "reload"), row(3, "x3", "reload")],
        "upsert": [row(1, "x1u", "upsert"), row(3, "x3", "upsert")],
        "delete": [row(1, "x1", "delete")],
        "none": [],
    }[mode]


# Eight sources in one batch, two per behaviour: every pair of behaviours (the same one twice included) meets in the
# same batch, and each source must end up as it would alone. The reversed run swaps which source gets which behaviour.
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reversed"])
@pytest.mark.parametrize("soft_delete", [False, True], ids=["hard", "soft"])
@pytest.mark.parametrize("cdc_cls", [SCD1, SCD2], ids=["scd1", "scd2"])
def test_each_source_follows_its_own_operations_whatever_the_other_sources_do(
    local_spark, cdc_cls, soft_delete, reverse
):
    behaviours = [mode for mode in MODES for _ in range(2)]
    if reverse:
        behaviours.reverse()
    sources = {f"s{i}": (10 * (i + 1), mode) for i, mode in enumerate(behaviours)}
    cdc = cdc_cls("cdc", f"multi_source_{cdc_cls.__name__}_{soft_delete}_{reverse}", spark=local_spark)
    options = {"keys": "id", "add_key": True, "soft_delete": soft_delete}

    seed = [(base + i, f"x{i}", source, "upsert", D1) for source, (base, _) in sources.items() for i in (1, 2)]
    cdc.update(local_spark.createDataFrame(seed, SCHEMA), **options)
    batch = [row for source, (base, mode) in sources.items() for row in _batch_rows(source, base, mode)]
    cdc.update(local_spark.createDataFrame(batch, SCHEMA), **options)

    current = cdc.table.dataframe
    if soft_delete or cdc_cls is SCD2:
        current = current.where("__is_current")
    expected = sorted((base + offset, name) for base, mode in sources.values() for offset, name in LIVE[mode].items())
    assert sorted((r.id, r.name) for r in current.select("id", "name").collect()) == expected, sources
