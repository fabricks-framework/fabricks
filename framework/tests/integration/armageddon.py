# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.scripts.armageddon import armageddon
from tests.integration._types import steps

# COMMAND ----------

print(steps)

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

armageddon(steps=steps)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore

# COMMAND ----------
