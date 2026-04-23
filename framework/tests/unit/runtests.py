# Databricks notebook source
# MAGIC %run ../integration/add_missing_modules

# COMMAND ----------

import sys
from logging import ERROR, INFO

import pytest
from databricks.sdk.runtime import dbutils

from fabricks.context import IS_TESTMODE
from fabricks.context.log import DEFAULT_LOGGER

# COMMAND ----------

assert IS_TESTMODE

# COMMAND ----------

Tests = ["test_git_path", "test_variable_substitution"]
Booleans = ["True", "False"]

# COMMAND ----------

DEFAULT_LOGGER.setLevel(INFO)

# COMMAND ----------

dbutils.widgets.multiselect("tests", "*", ["*"] + Tests)

# COMMAND ----------

tests = [t for t in dbutils.widgets.get("tests").split(",")]
if "*" in tests:
    tests = Tests

# COMMAND ----------

sys.dont_write_bytecode = True

# COMMAND ----------

DEFAULT_LOGGER.setLevel(ERROR)

# COMMAND ----------

k = " or ".join(tests)

# COMMAND ----------

res = pytest.main(
    [
        ".",
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
