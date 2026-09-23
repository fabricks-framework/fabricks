"""Reproduces https://github.com/fabricks-framework/fabricks/issues/182:
a silver job with `mode: latest` (nocdc) generates invalid SQL when the
incremental slice is empty -- on a table's first run, or on any scheduled
run where no new bronze rows have arrived since the last one.

Root cause (framework/fabricks/cdc/base/processor.py's fix_context() +
framework/fabricks/cdc/templates/filters/latest.sql.jinja): when the
source has no rows, `latest.sql.jinja`'s __latest CTE still produces
exactly one row (an ungrouped MAX(__timestamp) over zero rows is NULL,
not zero rows), and __final's `concat_ws(' ', ' (', concat(..., NULL,
...), ' )')` silently drops the NULL middle argument (concat_ws's
documented behavior), collapsing to the literal string " (  )" -- two
bare parens with nothing between them. `assert row.slices` in
fix_context() passes, because that non-empty string is truthy in Python,
so it gets injected verbatim into
framework/fabricks/cdc/templates/ctes/slice.sql.jinja's
`where true and ({{ slices }})`, producing `where true and ( () )`:
double-nested empty parens, which Databricks rejects with
PARSE_SYNTAX_ERROR.

This mirrors test_cdc_query_generation.py's pattern: construct a CDC
object directly with a mocked spark, and mock the fix_context probe's
own `spark.sql(...).collect()` result to the exact broken row Spark
returns for an empty slice (confirmed by rendering the real templates,
not guessed).

This test encodes the *expected* (fixed) behavior, not the current
broken one: it currently FAILS, and should turn green once fix_context
stops letting an empty slice through as invalid SQL.
"""

from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.cdc import NoCDC


def _fake_spark():
    return MagicMock(name="fake_spark")


def _src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    return df


def test_empty_latest_slice_does_not_generate_invalid_sql():
    spark = _fake_spark()
    # The row fix_context()'s probe query actually returns when the
    # source has no rows: __latest's ungrouped MAX(__timestamp) is NULL,
    # and concat_ws drops it, leaving bare parens with nothing between.
    spark.sql.return_value.collect.return_value = [Row(slices=" (  )", sources=None)]
    cdc = NoCDC("silver", "empty_slice", spark=spark)

    sql = cdc.get_query(_src(["id", "name"]), slice="latest")

    # Expected/fixed behavior: an empty slice must never produce
    # double-nested empty parens (invalid SQL Databricks rejects with
    # PARSE_SYNTAX_ERROR). Currently fails: fix_context()'s
    # `assert row.slices` passes (" (  )" is truthy), so the broken
    # value survives into the final query unchanged.
    normalized = " ".join(sql.split())
    assert "AND ( ()" not in normalized, (
        f"empty slice produced the known-broken double-nested-empty-parens shape:\n{sql}"
    )
