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

Fix: mirrors the existing `if slice == "update" and not has_rows: slice
= None` guard in Processor.get_query_context() -- a "latest" slice is
reset to None *before* fix_context() (and the parent_* CTE-chain
derivation that follows it) ever runs, whenever the source itself has
no rows. This avoids the fix_context probe query and its broken-string
defect entirely, rather than trying to detect and patch the broken
string after the fact (which would also need to retroactively fix up
every parent_rectify/parent_deduplicate_hash/parent_cdc reference that
already assumed "__sliced" exists).
"""

from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from fabricks.cdc import NoCDC


def _fake_spark():
    return MagicMock(name="fake_spark")


def _empty_src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    df.isEmpty.return_value = True
    return df


def test_empty_latest_slice_does_not_generate_invalid_sql():
    spark = _fake_spark()
    cdc = NoCDC("silver", "empty_slice", spark=spark)

    sql = cdc.get_query(_empty_src(["id", "name"]), slice="latest")

    # No __sliced CTE at all: get_query_context() reset slice=None
    # before rendering, so filter.sql.jinja's broken-empty-parens probe
    # never runs, and there's nothing left to produce invalid SQL from.
    assert "__sliced" not in sql
    normalized = " ".join(sql.split())
    assert "AND ( ()" not in normalized, (
        f"empty slice produced the known-broken double-nested-empty-parens shape:\n{sql}"
    )
