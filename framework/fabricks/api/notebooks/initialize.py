# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from databricks.sdk.runtime import dbutils, display

from fabricks.core.scripts import generate

# COMMAND ----------

dbutils.widgets.text("schedule", "---")

# COMMAND ----------

schedule = dbutils.widgets.get("schedule")
assert schedule != "---", "no schedule provided"

# COMMAND ----------

print(schedule)

# COMMAND ----------

schedule_id, job_df, dependency_df = generate(schedule=schedule)

# COMMAND ----------

display(job_df)

# COMMAND ----------

display(dependency_df)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="schedule_id", value=schedule_id)
dbutils.jobs.taskValues.set(key="schedule", value=schedule)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
