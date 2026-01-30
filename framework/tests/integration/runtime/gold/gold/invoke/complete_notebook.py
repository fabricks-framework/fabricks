# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from databricks.sdk.runtime import dbutils, spark

from fabricks.metastore.view import View

# COMMAND ----------


# COMMAND ----------

dbutils.widgets.text("step", "--")
dbutils.widgets.text("topic", "--")
dbutils.widgets.text("item", "--")

# COMMAND ----------

dbutils.widgets.text("arg1", "--")

# COMMAND ----------

step = dbutils.widgets.get("step")
topic = dbutils.widgets.get("topic")
item = dbutils.widgets.get("item")

# COMMAND ----------

arg1 = dbutils.widgets.get("arg1")

# COMMAND ----------

print(step, topic, item)

# COMMAND ----------

assert arg1 == "1"

# COMMAND ----------

df = spark.sql("select 1 as dummy")

# COMMAND ----------

uuid = View.create_or_replace(df)

# COMMAND ----------

dbutils.notebook.exit(uuid)  # type: ignore
