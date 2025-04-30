# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

import logging

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration._types import paths
from tests.integration.utils import git_to_landing, landing_to_raw

# COMMAND ----------

DEFAULT_LOGGER.setLevel(logging.DEBUG)

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

    if CATALOG is not None:
        spark.sql(f"use catalog {CATALOG}")
        spark.sql("drop schema if exists bronze cascade")
        spark.sql("create schema if not exists bronze")

    paths.raw.rm()
    paths.out.rm()

    git_to_landing()

# COMMAND ----------

if i:
    landing_to_raw(i)

# COMMAND ----------

for job in jobs:
    j = get_job(job=job)

    if drop and i == 1:
        j.drop()
        j.create()

    j.run()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
