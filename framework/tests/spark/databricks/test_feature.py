"""Follow-up checks for jobs the schedule already ran once.

Each job also runs in the schedule (conftest.py's _schedule_run); these tests do the extra action a single
schedule pass can't exercise (a repeat run, a hand-crafted follow-up batch). The "did it succeed" checks are in
test_schedule.py.
"""

from fabricks.context import SPARK
from fabricks.core import get_job


def test_bronze_feature_parser_streams_once_per_checkpoint():
    # The schedule already ran this job once; the checkpoint must make a second run a no-op.
    j = get_job(step="bronze", topic="feature", item="parser")
    first_count = SPARK.sql("select count(*) from bronze.feature_parser").collect()[0][0]

    j.run()

    df = SPARK.sql("select distinct _parsed_by from bronze.feature_parser")
    assert [r["_parsed_by"] for r in df.collect()] == ["dummy"]
    assert SPARK.sql("select count(*) from bronze.feature_parser").collect()[0][0] == first_count
    assert j.paths.to_checkpoints.exists()


def test_gold_type_widening_overwrite():
    # The schedule wrote an int batch; check the physical column type became double, not just that the value fit.
    j = get_job(step="gold", topic="type_widening", item="overwrite")

    df = SPARK.sql("select cast(field as double) as field from values (1.5), (2.5) as source(field)")
    j._for_each_batch(df)

    table = SPARK.table(j.table.qualified_name)
    assert table.schema["field"].dataType.simpleString() == "double"
    assert [row.field for row in table.orderBy("field").collect()] == [1.5, 2.5]


def test_gold_type_widening_merge():
    j = get_job(step="gold", topic="type_widening", item="merge")

    df = SPARK.sql("select __key, cast(field as double) as field from values ('two', 2.5) as source(__key, field)")
    j._for_each_batch(df)

    table = SPARK.table(j.table.qualified_name)
    assert table.schema["field"].dataType.simpleString() == "double"
    rows = table.select("__key", "field").orderBy("__key").collect()
    assert [(row["__key"], row.field) for row in rows] == [("one", 1.0), ("two", 2.5)]
