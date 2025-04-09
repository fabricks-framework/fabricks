# Databricks notebook source
import json

from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("step", "--")
dbutils.widgets.text("topic", "--")
dbutils.widgets.text("item", "--")

# COMMAND ----------

dbutils.widgets.text("arg1", "--")
dbutils.widgets.text("mode", "--")

# COMMAND ----------

step = dbutils.widgets.get("step")
topic = dbutils.widgets.get("topic")
item = dbutils.widgets.get("item")

# COMMAND ----------

arg1 = dbutils.widgets.get("arg1")
job_options = dbutils.widgets.get("job_options")
schedule_variables = dbutils.widgets.get("schedule_variables")

# COMMAND ----------

print(step, topic, item)

# COMMAND ----------

job_options = json.loads(job_options)
schedule_variables = json.loads(schedule_variables)

# COMMAND ----------

assert arg1 == "1", f"arg1 {arg1} <> 1"
assert job_options.get("mode") == "memory", f"mode {job_options.get('mode')} <> memory"
assert str(schedule_variables.get("var1")) == "1", f"var1 {schedule_variables.get('var1')} <> 1"


# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
