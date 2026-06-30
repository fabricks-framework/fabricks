# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

import sys
from logging import ERROR, INFO

import pytest
from databricks.sdk.runtime import dbutils

from fabricks.context import IS_TESTMODE, PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER, send_message_to_channel
from fabricks.utils.helpers import run_notebook
from fabricks.utils.pip import pip_list

# COMMAND ----------

assert IS_TESTMODE

# COMMAND ----------

Tests = ["job0", "job1", "job2", "job3", "job4", "job5"]
Booleans = ["True", "False"]

# COMMAND ----------

DEFAULT_LOGGER.setLevel(INFO)

# COMMAND ----------

_ = send_message_to_channel(
    channel="IT DWH Notifications",
    title="Test started",
    message="Test started",
    loglevel="DEBUG",
)

# COMMAND ----------

dbutils.widgets.dropdown("initialize", "True", Booleans)
dbutils.widgets.dropdown("armageddon", "True", Booleans)
dbutils.widgets.dropdown("reset", "False", Booleans)
dbutils.widgets.multiselect("tests", "*", ["*"] + Tests)

# COMMAND ----------

armageddon = dbutils.widgets.get("armageddon").lower() == "true"
initialize = dbutils.widgets.get("initialize").lower() == "true"
reset = dbutils.widgets.get("reset").lower() == "true"
tests = [t for t in dbutils.widgets.get("tests").split(",")]
if "*" in tests:
    tests = Tests

# COMMAND ----------

packages = pip_list(format="pyproject")
print(packages)

# COMMAND ----------

root = PATH_RUNTIME.parent().parent().joinpath("integration")
print(root)

# COMMAND ----------

if initialize:
    run_notebook(
        root.joinpath("initialize"),
        expected="True",
        i=1,
    )

# COMMAND ----------

if armageddon:
    run_notebook(root.joinpath("armageddon"))
elif reset:
    run_notebook(root.joinpath("reset"))

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
