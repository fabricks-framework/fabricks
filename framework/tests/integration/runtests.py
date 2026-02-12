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

# COMMAND ----------

assert IS_TESTMODE

# COMMAND ----------

Tests = ["job1", "job2", "job3", "job4", "job5"]
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
dbutils.widgets.dropdown("fix_notebooks", "True", Booleans)
dbutils.widgets.multiselect("tests", "*", ["*"] + Tests)

# COMMAND ----------

armageddon = dbutils.widgets.get("armageddon").lower() == "true"
initialize = dbutils.widgets.get("initialize").lower() == "true"
reset = dbutils.widgets.get("reset").lower() == "true"
fix_notebooks = dbutils.widgets.get("fix_notebooks").lower() == "true"
tests = [t for t in dbutils.widgets.get("tests").split(",")]
if "*" in tests:
    tests = Tests

# COMMAND ----------

if initialize:
    run_notebook(
        PATH_RUNTIME.parent().joinpath("initialize"),
        expected="True",
        i=1,
    )

# COMMAND ----------

if armageddon:
    run_notebook(PATH_RUNTIME.parent().joinpath("armageddon"))
elif reset:
    run_notebook(PATH_RUNTIME.parent().joinpath("reset"))

# COMMAND ----------

if fix_notebooks:
    run_notebook(PATH_RUNTIME.parent().joinpath("fix_notebooks"))

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
