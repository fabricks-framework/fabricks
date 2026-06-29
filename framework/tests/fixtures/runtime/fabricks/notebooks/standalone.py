# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from databricks.sdk.runtime import dbutils

from fabricks.core.schedules import standalone

# COMMAND ----------

dbutils.widgets.text("schedule", "---")

# COMMAND ----------

standalone()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
