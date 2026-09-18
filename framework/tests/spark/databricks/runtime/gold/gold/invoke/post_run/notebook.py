# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("arg1", "")

# COMMAND ----------

assert dbutils.widgets.get("arg1") == "1"

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
