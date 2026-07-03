# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils

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

# COMMAND ----------

init = dbutils.widgets.get("init").lower() == "true"

# COMMAND ----------

if init:
    paths.landing.rm()

# COMMAND ----------

paths.raw.rm()
paths.out.rm()

# COMMAND ----------

for d in ["bronze", "silver", "transf", "gold", "semantic", "fabricks"]:
    db = Database(d)
    db.drop()

# COMMAND ----------

if init:
    git_to_landing()

# COMMAND ----------

landing_to_raw(iter=[1])

# COMMAND ----------

if init:
    for d in ["expected", "input", "test"]:
        db = Database(d)
        db.drop()
        db.create()

    create_random_tables()
    create_expected_views()
    create_input_tables()

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
