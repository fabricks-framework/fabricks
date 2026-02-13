# Databricks notebook source

from databricks.sdk.runtime import dbutils

from fabricks.core.schedules import run

# COMMAND ----------

dbutils.widgets.text("step", "---")
dbutils.widgets.text("job_id", "---")
dbutils.widgets.text("schedule_id", "---")
dbutils.widgets.text("schedule", "---")

# COMMAND ----------

step = dbutils.widgets.get("step")
job_id = dbutils.widgets.get("job_id")

# COMMAND ----------

run(step=step, job_id=job_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
