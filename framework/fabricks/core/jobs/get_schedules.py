from typing import Any, Dict

from pyspark.sql import DataFrame
from sparkdantic import create_spark_schema

from fabricks.context import PATH_SCHEDULES, SPARK
from fabricks.models.schedule import Schedule
from fabricks.utils.read.read_yaml import read_yaml


def get_schedules():
    return read_yaml(PATH_SCHEDULES, root="schedule")


def get_schedule(name: str) -> Dict[str, Any]:
    schedule = next((s for s in get_schedules() if s.get("name") == name), None)
    assert schedule is not None, f"schedule not found: {name}"
    return schedule


def get_schedules_df() -> DataFrame:
    schema = create_spark_schema(Schedule)
    df = SPARK.createDataFrame(list(get_schedules()), schema=schema)

    assert df, "no schedules found"
    return df
