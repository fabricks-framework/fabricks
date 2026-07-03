# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils, spark

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.database import Database
from tests.integration._types import paths
from tests.integration.utils import (
    create_expected_views,
    create_input_tables,
    create_random_tables,
    git_to_landing,
    landing_to_raw,
)

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.dropdown("init", "False", ["True", "False"])
dbutils.widgets.dropdown("i", "1", ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])

# COMMAND ----------

init = dbutils.widgets.get("init").lower() == "true"
i = dbutils.widgets.get("i")
i = list(range(1, int(i) + 1))

# COMMAND ----------

if CATALOG:
    try:
        spark.sql(f"use catalog {CATALOG}")
        spark.sql("drop schema if exists bronze cascade")
        spark.sql("drop table if exists transf.fact_register")

    except Exception:
        pass

# COMMAND ----------

if init:
    paths.landing.rm()

# COMMAND ----------

paths.raw.rm()
paths.out.rm()

# COMMAND ----------

if init:
    git_to_landing()

# COMMAND ----------

if i:
    landing_to_raw(iter=i)

# COMMAND ----------

    for d in ["expected", "input", "test"]:
        db = Database(d)
        tables = db.get_tables()
        for t in tables.collect():
            if t["table"] is not None:
                spark.sql("drop table if exists " + t["table"])

# COMMAND ----------

# DBTITLE 1,Cell 11
if init:    
    for d in ["expected", "input", "test"]:
        db = Database(d)
        tables = db.get_tables()
        for t in tables:
            print(t)

    create_random_tables()

    create_expected_views()
    create_input_tables()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
