# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.errors.exceptions.base import IllegalArgumentException

from fabricks.core.scripts import process

# COMMAND ----------

dbutils.widgets.text("step", "---")
dbutils.widgets.text("schedule_id", "---")

# COMMAND ----------

try:
    schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
except (TypeError, IllegalArgumentException, ValueError):
    schedule_id = dbutils.widgets.get("schedule_id")
    assert schedule_id != "---", "no schedule_id provided"

assert schedule_id is not None

# COMMAND ----------

step = dbutils.widgets.get("step")
assert step != "---", "no step provided"

# COMMAND ----------

schedule = dbutils.widgets.get("schedule")
assert schedule != "---", "no schedule provided"

# COMMAND ----------

print(schedule_id)

# COMMAND ----------

print(step)

# COMMAND ----------

print(schedule)

# COMMAND ----------

process(schedule_id=schedule_id, schedule=schedule, step=step)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
