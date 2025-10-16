from typing import List, Optional, TypedDict

from pyspark.sql import DataFrame

from fabricks.context import PATH_SCHEDULES, SPARK
from fabricks.core.jobs.base._types import TStep
from fabricks.utils.read.read_yaml import read_yaml
from fabricks.utils.schema import get_schema_for_type


class Options(TypedDict):
    steps: Optional[List[TStep]]
    tag: Optional[str]
    view: Optional[str]
    variables: Optional[dict[str, str]]


class Schedule(TypedDict):
    name: str
    options: Options


def get_schedules():
    return read_yaml(PATH_SCHEDULES, root="schedule")


def get_schedules_df() -> DataFrame:
    schema = get_schema_for_type(Schedule)
    df = SPARK.createDataFrame(list(get_schedules()), schema=schema)  # type: ignore

    assert df, "no schedules found"
    return df
