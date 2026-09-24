"""https://github.com/fabricks-framework/fabricks/issues/202: the merge
query's target-side CTE (__current) is referenced by name from multiple
downstream consumers, each pruned to a different column subset -- which
defeats Spark's CTE-reuse detection and re-reads the target table from
storage once per consumer (14 times, measured) instead of once overall.

Processor._materialize_current_view() caches the batch-relevant (source-
filtered, when available) subset of the target under a stable global temp
view, once, and swaps `context["tgt"]` to point at it before query.sql.jinja
renders -- so every consumer of __current hits the same cached data instead
of re-scanning storage (see test_merge_query_target_scan_count.py in
tests/spark/apache for the real-Spark proof of that).

This checks the SQL-shape/plumbing with a mocked spark: the right
"uncache table" / "cache table" statements get issued, and the final query
references the cached view -- not the raw target table -- as its target.
"""

from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def _src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    df.isEmpty.return_value = False
    return df


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

    sql = cdc.get_query(_src(["id", "name", "__source"]), mode="update", slice="update", add_key=True, add_hash=True)

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
