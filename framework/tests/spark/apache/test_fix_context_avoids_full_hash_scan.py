"""Reproduces https://github.com/fabricks-framework/fabricks/issues/184:
Processor.fix_context's incremental-filter probe, for has_source=True
jobs (multiple sources feeding one target -- e.g. multiple bronze
parents), used to render the full filter.sql.jinja template chain,
which builds ctes/base.sql.jinja's __base CTE -- `select *, md5(<every
field>) as __hash, ...` over the whole batch -- even though the probe
only ever needs __source + a timestamp column. Confirmed directly
(outside this suite, not committed) that this blocks Spark's optimizer
from pruning __base away: a 2000-row, 32-column batch alone was enough
to OOM the driver from `.explain()`.

This runs the real fix_context() SQL (tests/unit/config/test_fix_context_
avoids_full_hash_scan.py checks the SQL shape with a mocked spark; this
checks it actually executes correctly against real Spark/Delta) for a
multi-source `mode: update` scenario and asserts the resulting
incremental filter is correct: a per-source slice that only lets through
rows newer than that source's own already-merged max timestamp. Kept
narrow (not wide/many-column) on purpose -- the actual merge step that
follows the probe still legitimately hashes every field for change
detection (unrelated to #184, and real work this tier's small test JVM
heap doesn't have room to spare on top of proving a separate point).
"""

from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def test_fix_context_update_probe_has_source_produces_correct_per_source_slice(local_spark):
    cdc = SCD1("cdc", "hash_scan_real", spark=local_spark)

    rows = [
        Row(id=1, name="a", __source="king", __timestamp="2024-01-01 00:00:00"),
        Row(id=2, name="b", __source="queen", __timestamp="2024-01-01 00:00:00"),
    ]
    df = local_spark.createDataFrame(rows)

    cdc.update(df, keys="id", add_key=True)
    assert cdc.table.dataframe.count() == 2

    # a follow-up batch: one row per source strictly newer than what's
    # already merged, plus a duplicate of the already-merged "king" row
    # (same id, same __timestamp) -- if the per-source slice were wrong
    # (e.g. missing a source, or comparing against the wrong source's
    # watermark), this would either re-touch the duplicate or miss one of
    # the genuinely new rows.
    rows2 = [
        Row(id=1, name="a", __source="king", __timestamp="2024-01-01 00:00:00"),
        Row(id=3, name="c", __source="king", __timestamp="2024-01-02 00:00:00"),
        Row(id=4, name="d", __source="queen", __timestamp="2024-01-02 00:00:00"),
    ]
    df2 = local_spark.createDataFrame(rows2)

    cdc.update(df2, keys="id", add_key=True)

    result = cdc.table.dataframe.select("id", "name").orderBy("id").collect()
    assert [(r.id, r.name) for r in result] == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
