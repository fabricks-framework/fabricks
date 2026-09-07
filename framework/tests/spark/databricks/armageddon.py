# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils
import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.deploy import Deploy
from tests.spark.databricks._types import steps

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

with pytest.raises(Exception) as exc_info:  # noqa: PT011 -- any failure is expected here, not a specific one
    Deploy.armageddon(steps=steps, nowait=True)  # why wait ?

print(f"armageddon failed as expected: {exc_info.value}")

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
