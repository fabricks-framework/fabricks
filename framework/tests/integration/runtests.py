# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

import sys
from logging import ERROR, INFO

import pytest
from databricks.sdk.runtime import dbutils

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER, send_message_to_channel
from fabricks.utils.helpers import run_notebook

# COMMAND ----------

DEFAULT_LOGGER.setLevel(INFO)

# COMMAND ----------

send_message_to_channel(
    channel="IT DWH Notifications",
    title="Test started",
    message="Test started",
    loglevel="INFO",
)

# COMMAND ----------

dbutils.widgets.dropdown("initialize", "True", ["True", "False"])
dbutils.widgets.dropdown("armageddon", "True", ["True", "False"])
dbutils.widgets.dropdown("reset", "False", ["True", "False"])
dbutils.widgets.multiselect("tests", "*", ["*", "job1", "job2", "job3", "job4"])

# COMMAND ----------

armageddon = dbutils.widgets.get("armageddon").lower() == "true"
initialize = dbutils.widgets.get("initialize").lower() == "true"
reset = dbutils.widgets.get("reset").lower() == "true"
tests = [t for t in dbutils.widgets.get("tests").split(",")]
if "*" in tests:
    tests = ["job1", "job2", "job3", "job4"]

# COMMAND ----------

if initialize:
    run_notebook(
        PATH_RUNTIME.parent().join("initialize"),
        expected="True",
        i=1,
    )

# COMMAND ----------

if armageddon:
    run_notebook(PATH_RUNTIME.parent().join("armageddon"))
elif reset:
    run_notebook(PATH_RUNTIME.parent().join("reset"))

# COMMAND ----------

sys.dont_write_bytecode = True

# COMMAND ----------

DEFAULT_LOGGER.setLevel(ERROR)

# COMMAND ----------

k = " or ".join(tests)

# COMMAND ----------

res = pytest.main(
    [
        "jobs",
        "-v",
        "-p",
        "no:cacheprovider",
        f"-k {k}",
    ]
)

# COMMAND ----------

assert res.value == 0, "failed"  # type: ignore

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
