"""Reproduces https://github.com/fabricks-framework/fabricks/issues/186:
Processor.fix_context's incremental-filter probe (cdc/templates/filters/
update.sql.jinja's __update CTE) computes MAX(target.__timestamp) with
`FROM <target> AS t WHERE TRUE` -- no predicate bounds which target rows
get read before the aggregate. On a table with hundreds of millions of
rows this single probe query is itself large enough to OOM executors,
regardless of how few new source rows actually need processing.

This captures the exact SQL fix_context sends to Spark (mirroring
test_cdc_query_generation.py's test_get_query_scd2_update_mode_takes_
incremental_branch pattern) and asserts it has that unbounded shape: the
target-table scan has no lower-bound/watermark predicate at all. It does
not attempt to reproduce the OOM itself (impractical at unit-test scale) --
the point is the query *shape* that causes it, which is independent of
target size.
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
            result.collect.return_value = [Row(slices=["s.__timestamp > '2024-01-01'"], sources=None)]
        return result

    spark.sql.side_effect = _sql
    cdc = SCD1("cdc", "query_gen", spark=spark)

    cdc.get_query(_src(["id", "name"]), mode="update", slice="update")

    # call 0 is Table.rows' "select count(*) ..."; call 1 is fix_context's probe
    probe_sql = spark.sql.call_args_list[1].args[0]

    assert re.search(r"max\(\s*`t`\.`__timestamp`\s*\)", probe_sql, re.IGNORECASE)
    assert re.search(r"from\s+`cdc`\.`query_gen`\s+as\s+`t`", probe_sql, re.IGNORECASE)

    # the defect: the __update CTE's own WHERE clause (which gates what the
    # target scan reads before computing MAX) is exactly "true" -- no
    # watermark/lower-bound predicate narrows it, so every row of the
    # target must be read to compute one MAX() per run.
    update_cte = re.search(r"`__update`\s+AS\s*\((.*?)\)\s*,\s*`__final`", probe_sql, re.IGNORECASE | re.DOTALL)
    assert update_cte, f"could not isolate __update CTE in:\n{probe_sql}"
    where_clause = re.search(r"WHERE(.*)$", update_cte.group(1), re.IGNORECASE | re.DOTALL)
    assert where_clause
    assert where_clause.group(1).strip().lower() == "true"
