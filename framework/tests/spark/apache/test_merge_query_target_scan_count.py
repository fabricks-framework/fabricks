"""Reproduces https://github.com/fabricks-framework/fabricks/issues/202:
a single `mode: update` merge query (SCD1) reads the target table many
times from storage instead of once, because __current -- although only
referenced once in the SQL text -- gets pruned to a different column
subset at each consumption site, defeating Spark's CTE-reuse detection.

Counts distinct physical `Scan parquet` node *definitions* for the target
table in the real physical plan (captured via explain(mode="formatted"),
same technique used to diagnose the issue) after a real
`cdc.get_data(..., mode="update")` call against a seeded Delta table. A node
definition line looks like `(14) Scan parquet ...target_scan_count`; the
same node is then referenced by id (`+- Scan parquet ...target_scan_count
(14)`) wherever the plan reuses it, so counting definitions -- not every
textual mention -- is what actually measures physical re-reads from
storage. Currently red: 28 definitions for this scenario, not 1.
"""

import contextlib
import io
import re

from pyspark.sql.types import Row

from fabricks.cdc import SCD1


def _explain_text(df):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


def test_update_merge_query_scans_target_table_once(local_spark):
    cdc = SCD1("cdc", "target_scan_count", spark=local_spark)

    seed_rows = [Row(id=i, name=f"n{i}", __source=f"src{i % 3}", __timestamp="2024-01-01 00:00:00") for i in range(50)]
    cdc.update(local_spark.createDataFrame(seed_rows), keys="id", add_key=True)

    batch_rows = [
        Row(id=i, name=f"n{i}v2", __source=f"src{i % 3}", __timestamp="2024-01-02 00:00:00") for i in range(50, 60)
    ]
    batch_df = local_spark.createDataFrame(batch_rows)

    df = cdc.get_data(batch_df, mode="update", keys="id", add_key=True)

    plan = _explain_text(df)
    scan_definitions = re.findall(r"^\(\d+\) Scan parquet .*target_scan_count", plan, re.MULTILINE)

    assert len(scan_definitions) <= 1, (
        f"target table scanned {len(scan_definitions)} times in one merge query plan "
        f"(issue #202) -- expected at most 1:\n{plan}"
    )
