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

Tests = ["0_armageddon", "1_schedule", "2_schedule", "3_run", "4_reload", "5_extra"]

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

dbutils.widgets.multiselect("tests", "*", ["*"] + Tests)

# COMMAND ----------

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

run_notebook(
    root.joinpath("initialize"),
    expected="True",
    i=1,
)

# COMMAND ----------

sys.dont_write_bytecode = True

# COMMAND ----------

DEFAULT_LOGGER.setLevel(ERROR)

# COMMAND ----------

k = " or ".join(tests)

# COMMAND ----------

res = pytest.main(
    [
        "tasks",
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
