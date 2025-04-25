# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG

from fabricks.context.log import Logger
from fabricks.core.scripts.armageddon import armageddon
from fabricks.utils.dbutils import dbutils
from tests.integration._types import steps

# COMMAND ----------

print(steps)

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

armageddon(steps=steps)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore

# COMMAND ----------
