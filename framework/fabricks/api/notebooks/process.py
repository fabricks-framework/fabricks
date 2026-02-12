# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from databricks.sdk.runtime import dbutils

from fabricks.core.schedules import process

# COMMAND ----------

dbutils.widgets.text("step", "---")
dbutils.widgets.text("schedule_id", "---")
dbutils.widgets.text("schedule", "---")

# COMMAND ----------

process()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
