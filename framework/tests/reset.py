# Databricks notebook source
from logging import DEBUG

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import Row

from framework.fabricks.context.log import Logger
from framework.fabricks.core import get_job
from framework.fabricks.utils.helpers import run_in_parallel
from framework.tests.types import paths
from framework.tests.utils import landing_to_raw

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

spark.sql("drop function if exists udf_identity")
spark.sql("drop function if exists udf_key")

# COMMAND ----------

paths.raw.rm()
landing_to_raw(1)

# COMMAND ----------


def _reset(row: Row):
    job = get_job(step=row["step"], job_id=row["job_id"])
    if job.paths.schema.exists():
        job.paths.schema.rm()
    if job.paths.checkpoints.exists():
        job.paths.checkpoints.rm()

    if job.mode == "memory":
        pass
    elif job.mode == "invoke":
        pass
    elif job.mode == "register":
        pass
    else:
        if job.table.exists():
            job.table.restore_to_version(0)
        else:
            job.create()


# COMMAND ----------

df = spark.sql("select * from framework.fabricks.jobs")

# COMMAND ----------

run_in_parallel(_reset, df, workers=16)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
