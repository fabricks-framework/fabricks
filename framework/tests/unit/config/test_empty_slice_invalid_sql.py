"""Reproduces https://github.com/fabricks-framework/fabricks/issues/182:
a silver `mode: latest` (nocdc) job must not generate invalid SQL when the
incremental slice is empty (first run, or no new bronze rows). `latest.sql.jinja` yields the literal
`" (  )"` for an empty source, which `fix_context()` injects as `where true and ( () )` (a Databricks
PARSE_SYNTAX_ERROR). The fix resets a "latest" slice to None before `fix_context()` runs whenever the
source has no rows, mirroring the existing `slice == "update" and not has_rows` guard.
"""

from fabricks.cdc import NoCDC
from tests.unit.config._helpers import fake_spark, src


def test_empty_latest_slice_does_not_generate_invalid_sql():
    spark = fake_spark()
    cdc = NoCDC("silver", "empty_slice", spark=spark)

    sql = cdc.get_query(src(["id", "name"], is_empty=True), slice="latest")

    # No __sliced CTE at all: get_query_context() reset slice=None
    # before rendering, so filter.sql.jinja's broken-empty-parens probe
    # never runs, and there's nothing left to produce invalid SQL from.
    assert "__sliced" not in sql
    normalized = " ".join(sql.split())
    assert "AND ( ()" not in normalized, (
        f"empty slice produced the known-broken double-nested-empty-parens shape:\n{sql}"
    )
