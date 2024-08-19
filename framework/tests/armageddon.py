# Databricks notebook source
from logging import DEBUG

from databricks.sdk.runtime import dbutils

from framework.fabricks.context.log import Logger
from framework.fabricks.core.scripts.armageddon import armageddon
from framework.tests.types import steps

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

armageddon(steps=steps)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
