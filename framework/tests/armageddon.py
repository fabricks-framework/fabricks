# Databricks notebook source
from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context.log import Logger
from fabricks.core.scripts.armageddon import armageddon
from tests.types import steps

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

armageddon(steps=steps)

# COMMAND ----------

dbutils.notebook.exit("exit (0)")  # type: ignore
