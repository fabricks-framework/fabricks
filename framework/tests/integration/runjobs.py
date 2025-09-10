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
i = int(i) if i != "0" else 1

# COMMAND ----------

jobs = dbutils.widgets.get("jobs")
assert jobs != "---"

# COMMAND ----------

jobs = [job.strip() for job in jobs.split(",")]

# COMMAND ----------

drop = dbutils.widgets.get("drop")
drop = drop.lower() == "true"

# COMMAND ----------


def move_files(iter: int):
    if iter == 1:
        paths.landing.rm()

        if CATALOG is not None:
            spark.sql(f"use catalog {CATALOG}")
            spark.sql("drop schema if exists bronze cascade")
            spark.sql("create schema if not exists bronze")

        paths.raw.rm()
        paths.out.rm()

        git_to_landing()

    landing_to_raw(iter)


# COMMAND ----------


def do(iter: int):
    move_files(iter)

    for job in jobs:
        j = get_job(job=job)

        if drop and iter == 1:
            j.drop()
            j.create()

        j.run()


# COMMAND ----------

for iter in range(1, i + 1):
    do(iter=iter)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore

# COMMAND ----------
