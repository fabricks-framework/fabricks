import pytest

from fabricks.core import get_job
from tests.spark.expected.compare import compare_to_expected, create_expected_views, load_expected


@pytest.fixture(scope="module", autouse=True)
def cdc_oracles(local_spark):
    for cdc in ("scd2", "scd1", "scd0"):
        if not local_spark.catalog.tableExists(f"expected.{cdc}_iter1"):
            create_expected_views(local_spark, cdc)


def _current_snapshot(spark, iteration: int):
    source = load_expected(spark, "scd1", iteration).where("__is_current and not __is_deleted")
    columns = ["id as __key", "id", "name", "doubleField", "__source"]
    if "newField" in source.columns:
        columns.append("newField")
    return source.selectExpr(*columns)


def _history_events(spark, iteration: int):
    source = load_expected(spark, "scd2", iteration)
    source.createOrReplaceTempView("__gold_scd2_source")
    columns = ", s.newField" if "newField" in source.columns else ""
    return spark.sql(f"""
        with dates as (
          select id, __source, __valid_from as __timestamp, 'upsert' as __operation
          from __gold_scd2_source
          union
          select id, __source, __valid_to as __timestamp, 'delete' as __operation
          from __gold_scd2_source
          where __is_deleted
        )
        select d.id as __key, s.id, s.name, s.doubleField, s.__source,
               d.__operation,
               if(d.__operation = 'delete', d.__timestamp + interval 1 second, d.__timestamp) as __timestamp
               {columns}
        from dates d
        left join __gold_scd2_source s
          on d.id = s.id
         and d.__source = s.__source
         and d.__timestamp >= s.__valid_from
         and d.__timestamp <= s.__valid_to
    """)


def test_gold_scd0_update_wiring(local_spark):
    job = get_job(step="gold", topic="cdc", item="scd0_update")
    second = _current_snapshot(local_spark, 2)

    job.for_each_batch(_current_snapshot(local_spark, 1))
    compare_to_expected(local_spark, table=job.table, cdc="scd0", iter=1, topic="king_and_queen")
    job.update_schema(second)
    job.for_each_batch(second)
    compare_to_expected(local_spark, table=job.table, cdc="scd0", iter=2, topic="king_and_queen")


def test_gold_scd1_complete_wiring(local_spark):
    job = get_job(step="gold", topic="cdc", item="scd1_complete")
    source = _current_snapshot(local_spark, 1)

    job.for_each_batch(source)

    rows = job.table.dataframe.orderBy("id").collect()
    assert [(row.id, row.name) for row in rows] == [
        (row.id, row.name) for row in source.orderBy("id").collect()
    ]
    assert all(row["__is_current"] and not row["__is_deleted"] for row in rows)


def test_gold_scd2_update_wiring(local_spark):
    job = get_job(step="gold", topic="cdc", item="scd2_update")
    first = _history_events(local_spark, 1)
    second = _history_events(local_spark, 2)

    job.for_each_batch(first)
    job.update_schema(second)
    job.for_each_batch(second)

    compare_to_expected(local_spark, table=job.table, cdc="scd2", iter=2, topic="king_and_queen")


def test_gold_truncate_reload_recovery(local_spark):
    job = get_job(step="gold", topic="cdc", item="recovery")
    initial = local_spark.createDataFrame([("1", 1, "one"), ("2", 2, "two")], ["__key", "id", "name"])
    recovered = local_spark.createDataFrame([("1", 1, "changed"), ("3", 3, "three")], ["__key", "id", "name"])

    job.for_each_batch(initial)
    job.truncate()
    assert job.table.dataframe.count() == 0

    recovered.createOrReplaceGlobalTempView("gold_cdc_recovery_source")
    job.run(reload=True, invoke=False)

    rows = job.table.dataframe.where("__is_current and not __is_deleted").orderBy("id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "changed"), (3, "three")]
