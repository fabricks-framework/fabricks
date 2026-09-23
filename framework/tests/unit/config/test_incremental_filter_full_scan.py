"""Reproduces https://github.com/fabricks-framework/fabricks/issues/186:
Processor.fix_context's incremental-filter probe (cdc/templates/
probe.sql.jinja's has_source=False "update" branch) computes
MAX(target.__timestamp) directly `FROM <target>` with no predicate at
all -- no lower-bound/watermark narrows which target rows get read
before the aggregate. On a table with hundreds of millions of rows this
single probe query is itself large enough to OOM executors, regardless
of how few new source rows actually need processing.

This captures the exact SQL fix_context sends to Spark (mirroring
test_cdc_query_generation.py's test_get_query_scd2_update_mode_takes_
incremental_branch pattern) and asserts it has that unbounded shape: the
target-table scan has no lower-bound/watermark predicate at all. It does
not attempt to reproduce the OOM itself (impractical at unit-test scale) --
the point is the query *shape* that causes it, which is independent of
target size.

NOT fixed by https://github.com/fabricks-framework/fabricks/issues/184's
fix (also to fix_context, but a different defect -- has_source=True
reading every column of the source unnecessarily): #184 replaced the
probe's SQL text (probe.sql.jinja instead of the old filter.sql.
jinja/filters/update.sql.jinja template chain), so this test's exact
regex assertions needed updating to match the new shape, but the
underlying defect this test documents -- no watermark on the target
scan -- is unchanged and still present in the new query too.
"""

import re
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def _fake_spark():
    return MagicMock(name="fake_spark")


def _src(columns):
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    return df


def test_fix_context_update_slice_probe_scans_full_target_unconditionally():
    spark = _fake_spark()

    def _sql(sql):
        result = MagicMock()
        if sql.strip().lower().startswith("select count(*)"):
            # Table.rows: only needs to be > 0 so slice="update" survives
            # Processor.get_query_context's `if slice == "update" and not
            # has_rows: slice = None` reset -- the actual count is
            # irrelevant to the bug (170M is the report's own figure).
            result.collect.return_value = [[170_000_000]]
        else:
            result.collect.return_value = [Row(slices="s.__timestamp > '2024-01-01'", sources=None)]
        return result

    spark.sql.side_effect = _sql
    cdc = SCD1("cdc", "query_gen", spark=spark)

    cdc.get_query(_src(["id", "name"]), mode="update", slice="update")

    # call 0 is Table.rows' "select count(*) ..."; call 1 is fix_context's probe
    probe_sql = spark.sql.call_args_list[1].args[0]

    assert re.search(r"max\(\s*`__timestamp`\s*\)", probe_sql, re.IGNORECASE)

    # the defect: the target scan has no WHERE clause -- no lower-bound/
    # watermark predicate narrows it at all, so every row of the target
    # must be read to compute one MAX() per run.
    from_clause = re.search(r"FROM\s+`cdc`\.`query_gen`\s*$", probe_sql, re.IGNORECASE | re.MULTILINE)
    assert from_clause, f"expected an unqualified, unconditional target scan in:\n{probe_sql}"
    assert "where" not in probe_sql.lower()
