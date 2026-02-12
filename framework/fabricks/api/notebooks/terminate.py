# Databricks notebook source

# COMMAND ----------

from databricks.sdk.runtime import dbutils

from fabricks.core.schedules import terminate

# COMMAND ----------

dbutils.widgets.text("schedule_id", "---")

# COMMAND ----------

terminate()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
