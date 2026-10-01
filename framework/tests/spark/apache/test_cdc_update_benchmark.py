"""Wall-clock benchmark of one `mode="update"` CDC merge query, SCD1 and SCD2.

Defaults to a small target so the regular suite stays fast; run it at a size where the
shuffles matter, serially (pytest-benchmark disables itself under xdist):

    BENCH_ROWS=400000 just test-apache tests/spark/apache/test_cdc_update_benchmark.py

Compare two template versions by running it on each and diffing the printed tables.
"""

import os

import pytest

from fabricks.cdc import SCD1, SCD2

ROWS = int(os.environ.get("BENCH_ROWS", "2000"))
BATCH = max(ROWS // 10, 2)


def _frame(spark, lo, hi, timestamp, tag):
    return spark.range(lo, hi).selectExpr(
        "cast(id as int) id",
        f"concat('{tag}', id) name",
        "concat('s', id % 3) __source",
        f"timestamp'{timestamp}' __timestamp",
    )


@pytest.mark.parametrize("cdc_cls", [SCD1, SCD2], ids=["scd1", "scd2"])
def test_update_query_wall_clock(benchmark, local_spark, cdc_cls):
    cdc = cdc_cls("cdc", f"bench_update_{cdc_cls.__name__.lower()}", spark=local_spark)
    cdc.update(_frame(local_spark, 0, ROWS, "2024-01-01 00:00:00", "n"), keys="id", add_key=True)
    # half the batch updates existing keys, half inserts new ones
    batch = _frame(local_spark, ROWS - BATCH // 2, ROWS + BATCH // 2, "2024-01-02 00:00:00", "v2")

    def run():
        df = cdc.get_data(batch, mode="update", keys="id", add_key=True)
        df.write.format("noop").mode("overwrite").save()

    benchmark.pedantic(run, rounds=7, warmup_rounds=1, iterations=1)
