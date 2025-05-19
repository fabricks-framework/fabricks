# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database
from tests.integration._types import paths
from tests.integration.utils import create_expected_views, create_random_tables, git_to_landing, landing_to_raw

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.dropdown("expected", "True", ["True", "False"])
dbutils.widgets.dropdown("rm", "True", ["True", "False"])
dbutils.widgets.dropdown("i", "1", ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])

# COMMAND ----------

expected = dbutils.widgets.get("expected").lower() == "true"
rm = dbutils.widgets.get("rm").lower() == "true"
i = dbutils.widgets.get("i")
i = list(range(1, int(i) + 1))

# COMMAND ----------

if CATALOG:
    try:
        spark.sql(f"use catalog {CATALOG}")
        spark.sql("drop schema if exists bronze cascade")
    except Exception:
        pass

# COMMAND ----------

if rm:
    paths.landing.rm()
    paths.raw.rm()
    paths.out.rm()

# COMMAND ----------

create_random_tables()

# COMMAND ----------

if rm:
    git_to_landing()

# COMMAND ----------

if i:
    landing_to_raw(iter=i)

# COMMAND ----------

if expected:
    db = Database("expected")
    db.drop()
    db.create()
    create_expected_views()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
