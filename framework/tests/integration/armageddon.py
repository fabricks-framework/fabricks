# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.deploy import Deploy
from tests.integration._types import steps

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

Deploy.armageddon(steps=steps, nowait=True)  # why wait ?

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
