# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("step", "")
dbutils.widgets.text("topic", "")
dbutils.widgets.text("item", "")
dbutils.widgets.text("arg1", "")

# COMMAND ----------

assert dbutils.widgets.get("step") == "gold"
assert dbutils.widgets.get("topic") == "invoke"
assert dbutils.widgets.get("item") == "notebook"
assert dbutils.widgets.get("arg1") == "1"

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
