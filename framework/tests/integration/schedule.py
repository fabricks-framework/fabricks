# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG
from typing import Any, cast

from databricks.sdk.runtime import dbutils, display

import fabricks.core.scripts as s
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.runtime import PATH_NOTEBOOKS
from fabricks.core import get_step
from fabricks.core.jobs.base._types import TStep
from fabricks.utils.helpers import run_in_parallel, run_notebook
from tests.integration._types import steps

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.text("schedule", "---")
dbutils.widgets.dropdown("terminate", "True", ["True", "False"])

# COMMAND ----------

terminate = dbutils.widgets.get("terminate").lower() == "true"
schedule = dbutils.widgets.get("schedule")
assert schedule != "---", "no schedule provided"

# COMMAND ----------

schedule_id, job_df, dependency_df = s.generate(schedule=schedule)

# COMMAND ----------

print(schedule_id)

# COMMAND ----------

display(job_df)

# COMMAND ----------

display(dependency_df)

# COMMAND ----------


def _schedule(task: Any):
    step = get_step(step=cast(TStep, task))
    run_notebook(
        PATH_NOTEBOOKS.join("process"),
        timeout=step.timeouts.step,
        step=task,
        schedule_id=schedule_id,
        schedule=schedule,
        workers=step.workers,
    )


# COMMAND ----------

run_in_parallel(_schedule, steps)

# COMMAND ----------

if terminate:
    s.terminate(schedule_id=schedule_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
