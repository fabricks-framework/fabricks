from databricks.sdk.runtime import dbutils
from pyspark.errors.exceptions.base import IllegalArgumentException

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.dags.terminator import DagTerminator


def terminate(schedule_id: str | None = None):
    """
    Terminate a schedule based on the given schedule ID.

    Args:
        schedule_id (str): The ID of the schedule to terminate. If None, it will be retrieved from task values or widgets.
    """
    if schedule_id is None:
        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    assert schedule_id is not None, "Schedule ID must be provided either as an argument, task value, or widget."
    DEFAULT_LOGGER.info(f"terminating schedule ({schedule_id})", extra={"label": "scheduler"})

    with DagTerminator(schedule_id=schedule_id) as t:
        t.terminate()
