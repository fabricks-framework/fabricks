from logging import DEBUG
from typing import Any

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step
from fabricks.core.schedules.generate import generate
from fabricks.core.schedules.terminate import terminate
from fabricks.utils.helpers import run_in_parallel, run_notebook


def standalone(schedule: str | None = None):
    """
    Run a schedule in standalone mode based on the given schedule.

    Args:
        schedule (str | None): The schedule to run. If None, it will be retrieved from task values or widgets.
    """

    DEFAULT_LOGGER.setLevel(DEBUG)

    if schedule is None:
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
