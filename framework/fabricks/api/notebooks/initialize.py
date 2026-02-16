# Databricks notebook source
from databricks.sdk.runtime import dbutils, display

from fabricks.core.schedules import generate

# COMMAND ----------

dbutils.widgets.text("schedule", "---")

# COMMAND ----------

_, job_df, dependency_df = generate()

# COMMAND ----------

display(job_df)

# COMMAND ----------

display(dependency_df)


# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
