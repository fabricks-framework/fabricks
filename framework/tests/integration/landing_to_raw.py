# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from tests.integration._types import paths
from tests.integration.utils import git_to_landing, landing_to_raw

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

if CATALOG:
    try:
        spark.sql(f"use catalog {CATALOG}")
        spark.sql("drop schema if exists bronze cascade")
        spark.sql("create schema bronze")

    except Exception:
        pass

# COMMAND ----------

paths.landing.rm()
git_to_landing()

# COMMAND ----------

paths.raw.rm()
landing_to_raw(1)

# COMMAND ----------

paths.out.rm()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
