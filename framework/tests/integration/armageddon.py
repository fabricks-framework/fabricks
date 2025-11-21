# Databricks notebook source
from logging import DEBUG

from _types import steps
from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.deploy import Deploy

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

Deploy.armageddon(steps=steps, nowait=True)  # why wait ?

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
