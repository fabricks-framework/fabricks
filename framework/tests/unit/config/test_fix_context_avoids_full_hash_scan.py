"""Reproduces https://github.com/fabricks-framework/fabricks/issues/184:
for `has_source=True` jobs, `Processor.fix_context`'s
incremental-filter probe must not build `__base` or `md5(...)` over the whole batch (that OOM'd the
driver on a 2000-row, 32-column batch). Asserts the SQL sent to Spark for the update and latest shapes,
with a mocked session. The real-Spark counterpart, which checks correctness, is
tests/spark/apache/test_fix_context_avoids_full_hash_scan.py.
"""

from unittest.mock import MagicMock

from pyspark.sql.types import Row

from fabricks.cdc import SCD1
from tests.unit.config._helpers import fake_spark, src


def test_fix_context_update_slice_probe_has_source_avoids_full_hash_scan():
    spark = fake_spark()

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

    cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="update", slice="update", add_key=True, add_hash=True
    )

    probe_sql = spark.sql.call_args_list[1].args[0]

    assert "__base" not in probe_sql, f"probe still builds __base:\n{probe_sql}"
    assert "md5(" not in probe_sql.lower(), f"probe still hashes every field:\n{probe_sql}"


def test_fix_context_latest_slice_probe_has_source_avoids_full_hash_scan():
    spark = fake_spark()
    spark.sql.return_value.collect.return_value = [
        Row(slices="( s.__timestamp == '2024-01-01' )", sources="t.__source == 'a'")
    ]
    cdc = SCD1("cdc", "hash_scan_probe_latest", spark=spark)

    cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="complete", slice="latest", add_key=True, add_hash=True
    )

    # call 0 is Table.rows' "select count(*) ..." (runs unconditionally in
    # get_query_context, not just for mode="update"); call 1 is the probe.
    probe_sql = spark.sql.call_args_list[1].args[0]

    assert "__base" not in probe_sql, f"probe still builds __base:\n{probe_sql}"
    assert "md5(" not in probe_sql.lower(), f"probe still hashes every field:\n{probe_sql}"
