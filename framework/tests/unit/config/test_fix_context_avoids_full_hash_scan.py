"""Reproduces https://github.com/fabricks-framework/fabricks/issues/184:
for `has_source=True` jobs, `Processor.fix_context`'s
incremental-filter probe must not build `__base` or `md5(...)` over the whole batch (that OOM'd the
driver on a 2000-row, 32-column batch). Asserts the SQL sent to Spark for the update and latest shapes,
with a faked session. The real-Spark counterpart, which checks correctness, is
tests/spark/apache/test_fix_context_avoids_full_hash_scan.py.
"""

from fabricks.cdc import SCD1
from tests.unit.config._helpers import probe_spark, sql_statements, src


def _probe_sql(spark) -> str:
    probes = [s for s in sql_statements(spark) if "as `slices`" in s.lower()]
    assert len(probes) == 1, f"expected exactly one incremental-filter probe, got {len(probes)}"
    return probes[0]


def _assert_probe_is_cheap(probe_sql: str) -> None:
    lowered = probe_sql.lower()
    assert "max(" in lowered, f"not the watermark probe:\n{probe_sql}"
    assert "__base" not in probe_sql, f"probe still builds __base:\n{probe_sql}"
    assert "md5(" not in lowered, f"probe still hashes every field:\n{probe_sql}"


def test_fix_context_update_slice_probe_has_source_avoids_full_hash_scan():
    spark = probe_spark()
    cdc = SCD1("cdc", "hash_scan_probe", spark=spark)

    cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="update", slice="update", add_key=True, add_hash=True
    )

    probe_sql = _probe_sql(spark)
    _assert_probe_is_cheap(probe_sql)
    assert "`__sources`" in probe_sql, "the per-source watermark must still be computed per distinct source"


def test_fix_context_latest_slice_probe_has_source_avoids_full_hash_scan():
    spark = probe_spark(slices="( s.__timestamp == '2024-01-01' )")
    cdc = SCD1("cdc", "hash_scan_probe_latest", spark=spark)

    cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="complete", slice="latest", add_key=True, add_hash=True
    )

    _assert_probe_is_cheap(_probe_sql(spark))
