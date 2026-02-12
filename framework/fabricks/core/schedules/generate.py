from typing import Tuple

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.dags.generator import DagGenerator


def generate(schedule: str | None = None) -> Tuple[str, DataFrame, DataFrame]:
    """
    Generate a schedule, job dataframe, and dependency dataframe based on the given schedule.

    Args:
        schedule (str | None): The schedule to generate from.

    Returns:
        Tuple[str, DataFrame, DataFrame]: A tuple containing the schedule ID, job dataframe, and dependency dataframe.
    """
    if schedule is None:
        schedule = dbutils.widgets.get("schedule")

        DEFAULT_LOGGER.info(f"generating {schedule}", extra={"label": "scheduler"})

    with DagGenerator(schedule) as g:
        schedule_id, job_df, dep_df = g.generate()

        DEFAULT_LOGGER.debug(f"generated {schedule} ({schedule_id})", extra={"label": "scheduler"})

        try:
            dbutils.jobs.taskValues.set(key="schedule_id", value=schedule_id)
            dbutils.jobs.taskValues.set(key="schedule", value=schedule)
        except Exception: # noqa: E722
            pass

        return schedule_id, job_df, dep_df
