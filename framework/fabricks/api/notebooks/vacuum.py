# Databricks notebook source
from databricks.sdk.runtime import dbutils
from fabricks.core.scripts import vacuum
from pyspark.errors.exceptions.base import IllegalArgumentException

# COMMAND ----------

dbutils.widgets.text("schedule_id", "---")

# COMMAND ----------

try:
    schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
except (TypeError, IllegalArgumentException, ValueError):
    schedule_id = dbutils.widgets.get("schedule_id")
    schedule_id = None if schedule_id == "---" else schedule_id

# COMMAND ----------

vacuum(schedule_id=schedule_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
