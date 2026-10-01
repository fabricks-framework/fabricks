"""Characterization test for https://github.com/fabricks-framework/fabricks/issues/186:
the `has_source=False` "update" probe
(`probe.sql.jinja`) computes MAX(target.__timestamp) with no predicate, so it scans the whole target.
Asserts the SQL shape only (the OOM is not reproducible at unit scale). Still current after #184
changed the probe's SQL text; delete when the probe gets a watermark.
"""

import re

from fabricks.cdc import SCD1
from tests.unit.config._helpers import probe_spark, sql_statements, src


def test_fix_context_update_slice_probe_scans_full_target_unconditionally():
    # the actual row count is irrelevant to the bug (170M is the report's own figure)
    spark = probe_spark(rows=170_000_000, slices="s.__timestamp > '2024-01-01'", sources=None)
    cdc = SCD1("cdc", "query_gen", spark=spark)

    cdc.get_query(src(["id", "name"]), mode="update", slice="update")

    # call 0 is Table.rows' "select count(*) ..."; call 1 is fix_context's probe
    probe_sql = sql_statements(spark)[1]

    assert re.search(r"max\(\s*`__timestamp`\s*\)", probe_sql, re.IGNORECASE)

    # the defect: the target scan has no WHERE clause -- no lower-bound/
    # watermark predicate narrows it at all, so every row of the target
    # must be read to compute one MAX() per run.
    from_clause = re.search(r"FROM\s+`cdc`\.`query_gen`\s*$", probe_sql, re.IGNORECASE | re.MULTILINE)
    assert from_clause, f"expected an unqualified, unconditional target scan in:\n{probe_sql}"
    assert "where" not in probe_sql.lower()
