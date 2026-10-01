"""Reproduces https://github.com/fabricks-framework/fabricks/issues/202:
one `mode: update` SCD1 merge query must read the target table once, not once per pruned
`__current` consumer (28 scans before the fix). Counts distinct `Scan parquet` nodes for the target in the plan.
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

    assert len(scan_definitions) == 1, (
        f"target table scanned {len(scan_definitions)} times in one merge query plan "
        f"(issue #202) -- expected exactly 1 (0 means the plan format changed and the regex matches nothing):\n{plan}"
    )
