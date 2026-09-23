"""Reproduces https://github.com/fabricks-framework/fabricks/issues/184:
Processor.fix_context's incremental-filter probe renders the full
filter.sql.jinja template chain, which always builds ctes/base.sql.jinja's
__base CTE (`select *, md5(<every field>) as __hash, ...`) over the whole
batch, even though the probe only ever needs __source + a timestamp column.

Empirically verified (not just asserted) before writing this test: with
has_source=False, Spark's own optimizer already fully prunes __base away
(confirmed via .explain() against the real templates -- the physical plan
never touches the source at all), so that shape needs no fix and is left
alone (see test_incremental_filter_full_scan.py, issue #186's
characterization test, which covers exactly that shape and is unaffected).

With has_source=True, filters/update.sql.jinja's `__update_source as
(select __source from {{ parent_slice }} group by __source)` --
right-joined against the target -- blocks that pruning: __base's md5()
hashing over every field actually gets planned and computed. Reproduced
directly: rendering the real templates with add_hash=True/add_key=True
(what an SCD1/SCD2 "update" job actually sets) against a 2000-row,
32-column synthetic batch OOM'd the driver from .explain() alone.

This captures the exact SQL fix_context sends to Spark for both
has_source=True shapes (update, latest) and asserts it does NOT reference
__base or md5(...) -- i.e. it never touches more than __source + the
relevant timestamp column of the source.
"""

from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def _fake_spark():
    return MagicMock(name="fake_spark")


def _src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    # Processor.has_data(src) -> not df.isEmpty(): a bare MagicMock's
    # .isEmpty() is truthy by default, which would make has_data() False
    # and silently reset slice="latest" to None (see get_query_context's
    # issue #182 guard) before fix_context ever runs.
    df.isEmpty.return_value = False
    return df


def test_fix_context_update_slice_probe_has_source_avoids_full_hash_scan():
    spark = _fake_spark()

    def _sql(sql):
        result = MagicMock()
        if sql.strip().lower().startswith("select count(*)"):
            # Table.rows: only needs to be > 0 so slice="update" survives
            # Processor.get_query_context's `if slice == "update" and not
            # has_rows: slice = None` reset.
            result.collect.return_value = [[10]]
        else:
            result.collect.return_value = [Row(slices="( s.__timestamp > '2024-01-01' )", sources="t.__source == 'a'")]
        return result

    spark.sql.side_effect = _sql
    cdc = SCD1("cdc", "hash_scan_probe", spark=spark)

    cdc.get_query(_src(["id", "name", "__source"]), mode="update", slice="update", add_key=True, add_hash=True)

    probe_sql = spark.sql.call_args_list[1].args[0]

    assert "__base" not in probe_sql, f"probe still builds __base:\n{probe_sql}"
    assert "md5(" not in probe_sql.lower(), f"probe still hashes every field:\n{probe_sql}"


def test_fix_context_latest_slice_probe_has_source_avoids_full_hash_scan():
    spark = _fake_spark()
    spark.sql.return_value.collect.return_value = [
        Row(slices="( s.__timestamp == '2024-01-01' )", sources="t.__source == 'a'")
    ]
    cdc = SCD1("cdc", "hash_scan_probe_latest", spark=spark)

    cdc.get_query(_src(["id", "name", "__source"]), mode="complete", slice="latest", add_key=True, add_hash=True)

    # call 0 is Table.rows' "select count(*) ..." (runs unconditionally in
    # get_query_context, not just for mode="update"); call 1 is the probe.
    probe_sql = spark.sql.call_args_list[1].args[0]

    assert "__base" not in probe_sql, f"probe still builds __base:\n{probe_sql}"
    assert "md5(" not in probe_sql.lower(), f"probe still hashes every field:\n{probe_sql}"
