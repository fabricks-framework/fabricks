"""Reproduces https://github.com/fabricks-framework/fabricks/issues/182:
a silver `mode: latest` (nocdc) job must not generate invalid SQL when the
incremental slice is empty (first run, or no new bronze rows). `latest.sql.jinja` yields the literal
`" (  )"` for an empty source, which `fix_context()` injects as `where true and ( () )` (a Databricks
PARSE_SYNTAX_ERROR). The fix resets a "latest" slice to None before `fix_context()` runs whenever the
source has no rows, mirroring the existing `slice == "update" and not has_rows` guard.
"""

from pyspark.sql.types import Row

from fabricks.cdc import NoCDC
from tests.unit.config._helpers import fake_spark, src


def test_empty_latest_slice_does_not_generate_invalid_sql():
    spark = fake_spark()
    cdc = NoCDC("silver", "empty_slice", spark=spark)

    sql = cdc.get_query(src(["id", "name"], is_empty=True), slice="latest")

    normalized = " ".join(sql.split())
    assert "__sliced" not in normalized, f"an empty slice must be reset before rendering:\n{sql}"


def test_non_empty_latest_slice_still_generates_the_sliced_cte():
    # Control for the guard above: if it fired for every source, the test above would pass for the wrong reason.
    spark = fake_spark()
    spark.sql.return_value.collect.return_value = [Row(slices="( s.__timestamp == '2024-01-01' )")]
    cdc = NoCDC("silver", "non_empty_slice", spark=spark)

    sql = cdc.get_query(src(["id", "name", "__timestamp"], is_empty=False), slice="latest")

    assert "__sliced" in " ".join(sql.split()), f"a non-empty latest slice lost its __sliced CTE:\n{sql}"
