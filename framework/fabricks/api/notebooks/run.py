# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

import json

from databricks.sdk.runtime import dbutils

from fabricks.core.dags.run import run

# COMMAND ----------

dbutils.widgets.text("step", "---")
dbutils.widgets.text("job_id", "---")
dbutils.widgets.text("job", "--")
dbutils.widgets.text("schedule_id", "---")
dbutils.widgets.text("schedule", "---")

# COMMAND ----------

step = dbutils.widgets.get("step")
assert step != "---"

# COMMAND ----------

job_id = dbutils.widgets.get("job_id")
assert job_id != "---"

# COMMAND ----------

job = dbutils.widgets.get("job")
assert job != "---"

# COMMAND ----------

schedule_id = dbutils.widgets.get("schedule_id")
assert schedule_id != "---"

# COMMAND ----------

schedule = dbutils.widgets.get("schedule")
assert schedule != "---"

# COMMAND ----------

context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())  # type: ignore
notebook_id = context.get("tags").get("jobId")
assert notebook_id is not None

# COMMAND ----------

run(step=step, job_id=job_id, schedule_id=schedule_id, schedule=schedule, notebook_id=notebook_id)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
