# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("step", "")
dbutils.widgets.text("topic", "")
dbutils.widgets.text("item", "")

# COMMAND ----------

step = dbutils.widgets.get("step")
topic = dbutils.widgets.get("topic")
item = dbutils.widgets.get("item")

# COMMAND ----------

assert step == "gold"
assert topic == "invoke"
assert item == "notebook_without_argument"

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
