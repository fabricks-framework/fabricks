# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("step", "")
dbutils.widgets.text("topic", "")
dbutils.widgets.text("item", "")

# COMMAND ----------

dbutils.widgets.text("arg1", "")

# COMMAND ----------

step = dbutils.widgets.get("step")
topic = dbutils.widgets.get("topic")
item = dbutils.widgets.get("item")

# COMMAND ----------

arg1 = dbutils.widgets.get("arg1")

# COMMAND ----------

assert step == "gold"
assert topic == "invoke"
assert item == "notebook"

# COMMAND ----------

assert arg1 == "1"

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
