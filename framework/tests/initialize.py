# Databricks notebook source
from logging import DEBUG

from databricks.sdk.runtime import dbutils
import sys
from fabricks.context import IS_TEST, PATH_RUNTIME
from fabricks.context.log import Logger
from fabricks.metastore.database import Database
framework_path = str(PATH_RUNTIME.pathlib.parent.parent.absolute())
print(framework_path)
sys.path.append(framework_path)
from tests.types import paths
from tests.utils import create_expected_views, git_to_landing, landing_to_raw

# COMMAND ----------

Logger.setLevel(DEBUG)

# COMMAND ----------

dbutils.widgets.dropdown("expected", "True", ["True", "False"])
dbutils.widgets.dropdown("i", "1", ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])

# COMMAND ----------

expected = dbutils.widgets.get("expected").lower() == "true"
i = dbutils.widgets.get("i")
i = list(range(1, int(i) + 1))

# COMMAND ----------

paths.landing.rm()
paths.raw.rm()
paths.out.rm()

# COMMAND ----------

git_to_landing()

# COMMAND ----------

if i:
    landing_to_raw(iter=i)

# COMMAND ----------

if expected:
    db = Database("expected")
    db.drop()
    db.create()
    create_expected_views()

# COMMAND ----------

dbutils.notebook.exit("exit (0)")
