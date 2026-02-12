# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from databricks.sdk.runtime import dbutils, display

from fabricks.core.schedules import generate

# COMMAND ----------

dbutils.widgets.text("schedule", "---")

# COMMAND ----------

schedule_id, job_df, dependency_df = generate()

# COMMAND ----------

display(job_df)

# COMMAND ----------

display(dependency_df)


# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
