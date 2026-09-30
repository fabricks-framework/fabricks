"""https://github.com/fabricks-framework/fabricks/issues/202:
the merge query caches the batch-relevant target subset under a
stable global temp view, so every `__current` consumer reads it once instead of re-scanning storage.
Checks the plumbing with a mocked session: one uncache, one cache, and a final query that targets the
cached view rather than the raw table. The scan-count proof is
tests/spark/apache/test_merge_query_target_scan_count.py.
"""

from unittest.mock import MagicMock

from pyspark.sql.types import Row

from fabricks.cdc import SCD1
from tests.unit.config._helpers import src


def _fake_spark():
    spark = MagicMock(name="fake_spark")

    def _sql(sql):
        result = MagicMock()
        lowered = sql.strip().lower()
        if lowered.startswith("select count(*)"):
            result.collect.return_value = [[10]]
        elif "slices" in lowered and "sources" in lowered:
            result.collect.return_value = [Row(slices="( s.__timestamp > '2024-01-01' )", sources="t.__source == 'a'")]
        return result

    spark.sql.side_effect = _sql
    return spark


def test_update_query_targets_the_cached_view_not_the_raw_table():
    spark = _fake_spark()
    cdc = SCD1("cdc", "target_cache_shape", spark=spark)

    sql = cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="update", slice="update", add_key=True, add_hash=True
    )

    calls = [c.args[0] for c in spark.sql.call_args_list]
    uncache_calls = [c for c in calls if c.strip().lower().startswith("uncache table")]
    cache_calls = [c for c in calls if c.strip().lower().startswith("cache table")]

    assert len(uncache_calls) == 1, f"expected exactly one uncache, got:\n{calls}"
    assert len(cache_calls) == 1, f"expected exactly one cache, got:\n{calls}"
    assert "global_temp." in cache_calls[0], cache_calls[0]
    assert "__current" in cache_calls[0], cache_calls[0]

    assert "global_temp" in sql, f"final query does not reference the cached view:\n{sql}"
    assert "__current" in sql, f"final query does not reference the cached view:\n{sql}"
    assert "`target_cache_shape`" not in sql, f"final query still reads the raw target directly:\n{sql}"
