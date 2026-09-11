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


def test_gold_nocdc_update_wiring(local_spark):
    job = get_job(step="gold", topic="cdc", item="nocdc_update")
    first = local_spark.createDataFrame(
        [("1", 1, "one", "2024-01-01 00:00:00", "9999-12-31 00:00:00", True, False)],
        ["__key", "id", "name", "__valid_from", "__valid_to", "__is_current", "__is_deleted"],
    )
    second = local_spark.createDataFrame(
        [
            ("1", 1, "changed", "2024-01-02 00:00:00", "9999-12-31 00:00:00", True, False),
            ("2", 2, "two", "2024-01-02 00:00:00", "9999-12-31 00:00:00", True, False),
        ],
        ["__key", "id", "name", "__valid_from", "__valid_to", "__is_current", "__is_deleted"],
    )

    job.for_each_batch(first)
    job.for_each_batch(second)

    rows = job.table.dataframe.orderBy("id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "changed"), (2, "two")]


@pytest.mark.parametrize(
    ("item", "target_timestamp_column"),
    [("last_timestamp_scd1", "__timestamp"), ("last_timestamp_scd2", "__valid_from")],
)
def test_gold_persists_last_timestamp(local_spark, item, target_timestamp_column):
    job = get_job(step="gold", topic="cdc", item=item)
    source_name = f"gold_cdc_{item}_source"
    first = local_spark.createDataFrame(
        [("1", 1, "one", "upsert", "2024-01-01 00:00:00")],
        ["__key", "id", "name", "__operation", "__timestamp"],
    )
    second = local_spark.createDataFrame(
        [("1", 1, "changed", "upsert", "2024-01-02 00:00:00")],
        ["__key", "id", "name", "__operation", "__timestamp"],
    )

    first.createOrReplaceGlobalTempView(source_name)
    job.create()
    job.run(invoke=False)
    second.createOrReplaceGlobalTempView(source_name)
    job.run(invoke=False)

    rows = job.cdc_last_timestamp.table.dataframe.collect()
    target_timestamp = job.table.dataframe.selectExpr(f"max({target_timestamp_column}) as value").collect()[0].value
    assert [(row["__timestamp"], row.asDict().get("__source")) for row in rows] == [
        (target_timestamp, None)
    ]


def test_manual_gold_job_executes_when_run_directly(local_spark):
    job = get_job(step="gold", topic="cdc", item="manual")
    source = local_spark.createDataFrame([(1, "manual")], ["id", "name"])

    job.for_each_batch(source)

    assert [(row.id, row.name) for row in job.table.dataframe.collect()] == [(1, "manual")]


def test_gold_scd2_update_wiring(local_spark):
    update_job = get_job(step="gold", topic="cdc", item="scd2_update")
    complete_job = get_job(step="gold", topic="cdc", item="scd2_complete")
    first = local_spark.createDataFrame(
        [("1", 1, "one", "upsert", "2024-01-01 00:00:00")],
        ["__key", "id", "name", "__operation", "__timestamp"],
    )
    second = local_spark.createDataFrame(
        [("1", 1, "changed", "upsert", "2024-01-02 00:00:00")],
        ["__key", "id", "name", "__operation", "__timestamp"],
    )

    update_job.for_each_batch(first)
    update_job.for_each_batch(second)
    complete_job.for_each_batch(first)
    complete_job.for_each_batch(second)

    rows = update_job.table.dataframe.orderBy("__valid_from").collect()
    assert [(row.name, row["__is_current"], row["__is_deleted"]) for row in rows] == [
        ("one", False, False),
        ("changed", True, False),
    ]
    rows = complete_job.table.dataframe.orderBy("__valid_from").collect()
    assert [(row.name, row["__is_current"], row["__is_deleted"]) for row in rows] == [("changed", True, False)]


def test_gold_truncate_reload_recovery(local_spark):
    job = get_job(step="gold", topic="cdc", item="recovery")
    initial = local_spark.createDataFrame([("1", 1, "one"), ("2", 2, "two")], ["__key", "id", "name"])
    recovered = local_spark.createDataFrame([("1", 1, "changed"), ("3", 3, "three")], ["__key", "id", "name"])

    initial.createOrReplaceGlobalTempView("gold_cdc_recovery_source")
    job.create()
    job.run(invoke=False)
    job.truncate()
    assert job.table.dataframe.count() == 0

    recovered.createOrReplaceGlobalTempView("gold_cdc_recovery_source")
    job.run(reload=True, invoke=False)

    rows = job.table.dataframe.orderBy("id").collect()
    assert [(row.id, row.name, row["__is_current"], row["__is_deleted"]) for row in rows] == [
        (1, "changed", True, False),
        (3, "three", True, False),
    ]
