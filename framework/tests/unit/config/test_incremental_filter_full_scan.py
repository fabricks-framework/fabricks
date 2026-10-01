"""https://github.com/fabricks-framework/fabricks/issues/186: the `has_source=False` "update" probe
(`probe.sql.jinja`) computes MAX(target.__timestamp) with no predicate, so it scans the whole target. Asserts the
SQL shape only (the OOM is not reproducible at unit scale). Encodes the desired behaviour as a strict xfail: it
goes red when the probe gets a watermark, which is the cue to drop the marker."""

import re

import pytest

from fabricks.cdc import SCD1
from tests.unit.config._helpers import probe_spark, sql_statements, src


@pytest.mark.xfail(strict=True, reason="probe.sql.jinja scans the whole target (#186)")
def test_fix_context_update_slice_probe_is_bounded_by_a_watermark():
    # the actual row count is irrelevant to the bug (170M is the report's own figure)
    spark = probe_spark(rows=170_000_000, slices="s.__timestamp > '2024-01-01'", sources=None)
    cdc = SCD1("cdc", "query_gen", spark=spark)

    cdc.get_query(src(["id", "name"]), mode="update", slice="update")

    # call 0 is Table.rows' "select count(*) ..."; call 1 is fix_context's probe
    probe_sql = sql_statements(spark)[1]

    assert re.search(r"max\(\s*`__timestamp`\s*\)", probe_sql, re.IGNORECASE)

    assert "where" in probe_sql.lower(), f"the probe still reads the whole target:\n{probe_sql}"
