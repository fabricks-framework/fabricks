# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

import json

from databricks.sdk.runtime import dbutils

from fabricks.core.dags.log import DagsLogger, DagsTableLogger
from fabricks.core.jobs import get_job

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

# COMMAND ----------

job = get_job(step=step, job_id=job_id)

# COMMAND ----------

print(job.qualified_name)

# COMMAND ----------

extra = {
    "partition_key": schedule_id,
    "schedule_id": schedule_id,
    "schedule": schedule,
    "step": step,
    "job": job,
    "notebook_id": notebook_id,
    "target": "buffer",
}

# COMMAND ----------

DagsLogger.info("running", extra=extra)


# COMMAND ----------

try:
    job.run(schedule_id=schedule_id, schedule=schedule)
    DagsLogger.info("done", extra=extra)

except Exception as e:
    DagsLogger.exception("failed", extra=extra)
    raise e

finally:
    DagsTableLogger.flush()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
