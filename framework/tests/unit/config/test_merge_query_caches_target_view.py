"""https://github.com/fabricks-framework/fabricks/issues/202:
the merge query caches the batch-relevant target subset under a stable global temp view, so every `__current`
consumer reads it once instead of re-scanning storage. Checks the plumbing with a mocked session: one uncache,
one cache, and a final query on the cached view. The scan-count proof is
tests/spark/apache/test_merge_query_target_scan_count.py.
"""

import re

from fabricks.cdc import SCD1
from tests.unit.config._helpers import probe_spark, sql_statements, src

_CACHE = r"\s*cache (?:lazy )?table (\S+)"


def _cache_view(statement: str) -> str:
    match = re.match(_CACHE, statement, re.IGNORECASE)
    assert match, statement
    return match.group(1)


def test_update_query_targets_the_cached_view_not_the_raw_table():
    spark = probe_spark()
    cdc = SCD1("cdc", "target_cache_shape", spark=spark)

    sql = cdc.get_query(
        src(["id", "name", "__source"], is_empty=False), mode="update", slice="update", add_key=True, add_hash=True
    )

    calls = sql_statements(spark)
    uncache_calls = [c for c in calls if c.strip().lower().startswith("uncache table")]
    cache_calls = [c for c in calls if re.match(_CACHE, c, re.IGNORECASE)]
    assert len(uncache_calls) == 1, f"expected exactly one uncache, got:\n{calls}"
    assert len(cache_calls) == 1, f"expected exactly one cache, got:\n{calls}"
    assert calls.index(uncache_calls[0]) < calls.index(cache_calls[0]), (
        "the stale view must be dropped before recaching"
    )

    view = _cache_view(cache_calls[0])
    assert view.startswith("global_temp."), view
    assert view.endswith("__current"), view
    assert view.removeprefix("global_temp.") in sql, f"final query does not read the cached view {view}:\n{sql}"
    assert "`target_cache_shape`" not in sql, f"final query still reads the raw target directly:\n{sql}"


def test_complete_query_does_not_cache_the_target():
    # Control: the cache plumbing belongs to the incremental path only.
    spark = probe_spark()
    cdc = SCD1("cdc", "target_cache_complete", spark=spark)

    cdc.get_query(src(["id", "name", "__source"], is_empty=False), mode="complete", add_key=True, add_hash=True)

    assert [c for c in sql_statements(spark) if re.match(_CACHE, c, re.IGNORECASE)] == []
