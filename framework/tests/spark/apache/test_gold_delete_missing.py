"""Issue #251: a gold nocdc `mode: update` job with `delete_missing: true` hard deletes keys absent from the source;
without the option the same run keeps them."""

import pytest

from fabricks.core import get_job

COLUMNS = ["__key", "id", "name"]


@pytest.mark.parametrize(
    ("item", "expected"),
    [("nocdc_update_delete_missing", [(1, "changed")]), ("nocdc_update", [(1, "changed"), (2, "two")])],
)
def test_gold_nocdc_update_delete_missing(local_spark, item, expected):
    job = get_job(step="gold", topic="cdc", item=item)
    first = local_spark.createDataFrame([("1", 1, "one"), ("2", 2, "two")], COLUMNS)
    second = local_spark.createDataFrame([("1", 1, "changed")], COLUMNS)

    job.for_each_batch(first)
    job.for_each_batch(second)

    rows = job.table.dataframe.orderBy("id").collect()
    assert [(row.id, row.name) for row in rows] == expected
