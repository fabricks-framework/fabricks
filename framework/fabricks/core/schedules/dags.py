from typing import Any, Tuple

from databricks.sdk.runtime import dbutils, spark
from pyspark.errors.exceptions.base import IllegalArgumentException
from pyspark.sql import DataFrame

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step
from fabricks.core.dags import DagGenerator, DagProcessor, DagTerminator, run
from fabricks.utils.helpers import run_in_parallel, run_notebook


def standalone(schedule: str | None = None):
    """
    Run a schedule in standalone mode based on the given schedule.

    Args:
        schedule (str | None): The schedule to run. If None, it will be retrieved from task values or widgets.
    """

    if schedule is None:
        DEFAULT_LOGGER.debug("schedule not provided, trying task value or widget", extra={"label": "scheduler"})
        schedule = dbutils.widgets.get("schedule")

    schedule_id, job_df, _ = generate(schedule=schedule)
    steps = [row.step for row in spark.sql("select step from {df} group by step", df=job_df).collect()]

    def _schedule(task: Any):
        step = get_step(step=task)
        run_notebook(
            PATH_NOTEBOOKS.joinpath("process"),
            timeout=step.timeouts.step,
            step=task,
            schedule_id=schedule_id,
            schedule=schedule,
        )

    _ = run_in_parallel(_schedule, steps)

    terminate(schedule_id=schedule_id)


def terminate(schedule_id: str | None = None):
    """
    Terminate a schedule based on the given schedule ID.

    Args:
        schedule_id (str): The ID of the schedule to terminate. If None, it will be retrieved from task values or widgets.
    """
    if schedule_id is None:
        DEFAULT_LOGGER.debug("schedule_id not provided, trying task value or widget", extra={"label": "scheduler"})
        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    assert schedule_id is not None, "Schedule ID must be provided either as an argument, task value, or widget."
    DEFAULT_LOGGER.info(f"terminating schedule ({schedule_id})", extra={"label": "scheduler"})

    with DagTerminator(schedule_id=schedule_id) as t:
        t.terminate()


def process(step: str | None = None, schedule_id: str | None = None, schedule: str | None = None):
    """
    Process a schedule based on the given schedule ID, schedule, and step.

    Args:
        schedule_id (str | None): The ID of the schedule to process. If None, it will be retrieved from task values or widgets.
        schedule (str | None): The schedule to process. If None, it will be retrieved from task values or widgets.
        step (str | None): The step to process. If None, it will be retrieved from widgets.
    """
    if schedule_id is None:
        DEFAULT_LOGGER.debug("schedule_id not provided, trying task value or widget", extra={"label": "scheduler"})
        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    assert schedule_id is not None, "schedule_id must be provided either as an argument, task value, or widget."

    if schedule is None:
        DEFAULT_LOGGER.debug("schedule not provided, trying task value or widget", extra={"label": "scheduler"})
        try:
            schedule = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule = dbutils.widgets.get("schedule")

    assert schedule is not None, "schedule must be provided either as an argument, task value, or widget."

    if step is None:
        DEFAULT_LOGGER.debug("step not provided, trying widgets", extra={"label": "scheduler"})
        step = dbutils.widgets.get("step")

    assert step is not None, "step must be provided either as an argument or widget."

    DEFAULT_LOGGER.info(f"processing {step} in {schedule} ({schedule_id})", extra={"label": "scheduler"})

    with DagProcessor(schedule_id=schedule_id, schedule=schedule, step=step) as p:
        p.process()


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
        except Exception:  # noqa: E722
            DEFAULT_LOGGER.warning(
                "could not set task values for schedule_id and schedule", extra={"label": "scheduler"}
            )
            DEFAULT_LOGGER.debug(
                "use widgets or parameters to pass schedule_id and schedule to downstream tasks",
                extra={"label": "scheduler"},
            )

        return schedule_id, job_df, dep_df


__all__ = ["process", "generate", "run", "terminate", "standalone"]
