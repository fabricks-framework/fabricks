# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context.log import Logger
from fabricks.core import get_job
from tests.integration._types import paths
from tests.integration.utils import git_to_landing, landing_to_raw

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.dropdown("i", "1", [str(i) for i in list(range(0, 13))])
dbutils.widgets.text("jobs", "---")
dbutils.widgets.dropdown("drop", "True", ["True", "False"])

# COMMAND ----------

i = dbutils.widgets.get("i")
i = int(i) if i != "0" else None

# COMMAND ----------

jobs = dbutils.widgets.get("jobs")
assert jobs != "---"

# COMMAND ----------

jobs = [job.strip() for job in jobs.split(",")]

# COMMAND ----------

drop = dbutils.widgets.get("drop")
drop = drop.lower() == "true"

# COMMAND ----------

if i == 1:
    paths.landing.rm()
    paths.raw.rm()
    paths.out.rm()

    git_to_landing()

# COMMAND ----------

if i:
    landing_to_raw(i)

# COMMAND ----------

for job in jobs:
    j = get_job(job=job)

    if drop:
        j.drop()
        j.create()

    j.run()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
