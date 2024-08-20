# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.errors.exceptions.base import IllegalArgumentException

from fabricks.core.scripts import terminate

# COMMAND ----------

try:
    schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
except (TypeError, IllegalArgumentException, ValueError):
    schedule_id = dbutils.widgets.get("schedule_id")
    assert schedule_id != "---", "no schedule_id provided"

assert schedule_id is not None

# COMMAND ----------

print(schedule_id)

# COMMAND ----------

terminate(schedule_id=schedule_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
