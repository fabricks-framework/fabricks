# Databricks notebook source
from logging import DEBUG

from databricks.sdk.runtime import dbutils
import sys
from fabricks.context.log import Logger, PATH_RUNTIME
from fabricks.core.scripts.armageddon import armageddon
framework_path = str(PATH_RUNTIME.pathlib.parent.parent.absolute())
print(framework_path)
sys.path.append(framework_path)
from tests.types import steps

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

armageddon(steps=steps)

# COMMAND ----------

dbutils.notebook.exit("exit (0)")
