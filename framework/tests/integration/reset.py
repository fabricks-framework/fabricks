# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.types import Row

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.utils.helpers import run_in_parallel
from tests.integration._types import paths
from tests.integration.utils import landing_to_raw

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

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

df = spark.sql("select * from fabricks.jobs")

# COMMAND ----------

run_in_parallel(_reset, df, workers=16)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
