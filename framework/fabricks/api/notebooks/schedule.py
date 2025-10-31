# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG
from typing import Any, cast

from databricks.sdk.runtime import dbutils, display, spark

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_step
from fabricks.core.jobs.base._types import TStep
from fabricks.core.schedules import generate, terminate
from fabricks.utils.helpers import run_in_parallel, run_notebook

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.text("schedule", "---")

# COMMAND ----------

schedule = dbutils.widgets.get("schedule")
assert schedule != "---", "no schedule provided"

# COMMAND ----------

schedule_id, job_df, dependency_df = generate(schedule=schedule)

# COMMAND ----------

print(schedule_id)

# COMMAND ----------

display(job_df)

# COMMAND ----------

display(dependency_df)

# COMMAND ----------
steps = [row.step for row in spark.sql("select step from {df} group by step", df=job_df).collect()]

# COMMAND ----------


def _schedule(task: Any):
    step = get_step(step=cast(TStep, task))
    run_notebook(
        PATH_NOTEBOOKS.joinpath("process"),
        timeout=step.timeouts.step,
        step=task,
        schedule_id=schedule_id,
        schedule=schedule,
        workers=step.workers,
    )


# COMMAND ----------

run_in_parallel(_schedule, steps)

# COMMAND ----------

terminate(schedule_id=schedule_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
