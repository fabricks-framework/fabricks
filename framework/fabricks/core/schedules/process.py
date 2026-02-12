from databricks.sdk.runtime import dbutils
from pyspark.errors.exceptions.base import IllegalArgumentException

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.dags.processor import DagProcessor


def process(step: str | None = None, schedule_id: str | None = None, schedule: str | None = None):
    """
    Process a schedule based on the given schedule ID, schedule, and step.

    Args:
        schedule_id (str | None): The ID of the schedule to process. If None, it will be retrieved from task values or widgets.
        schedule (str | None): The schedule to process. If None, it will be retrieved from task values or widgets.
        step (str | None): The step to process. If None, it will be retrieved from widgets.
    """
    if schedule_id is None:
        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    if schedule is None:
        try:
            schedule = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule = dbutils.widgets.get("schedule")

    if step is None:
        step = dbutils.widgets.get("step")

    assert schedule_id is not None, "schedule_id must be provided either as an argument, task value, or widget."
    assert schedule is not None, "schedule must be provided either as an argument, task value, or widget."
    assert step is not None, "step must be provided either as an argument or widget."

    DEFAULT_LOGGER.info(f"processing {step} in {schedule} ({schedule_id})", extra={"label": "scheduler"})

    with DagProcessor(schedule_id=schedule_id, schedule=schedule, step=step) as p:
        p.process()
