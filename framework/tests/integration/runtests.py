# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

import sys
from logging import ERROR, INFO

import pytest
from databricks.sdk.runtime import dbutils

from fabricks.context import IS_TESTMODE
from fabricks.context.log import DEFAULT_LOGGER, send_message_to_channel
from fabricks.utils.helpers import run_notebook
from fabricks.utils.pip import pip_list
from tests.integration.helpers.const import PHASES, ROOT

# COMMAND ----------

assert IS_TESTMODE

# COMMAND ----------

Phases = sorted(p.name for p in PHASES.pathlibpath.glob("[0-9]_*") if p.is_dir())

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

dbutils.widgets.multiselect("phases", "*", ["*"] + Phases)

# COMMAND ----------

phases = [t for t in dbutils.widgets.get("phases").split(",")]
if "*" in phases:
    phases = Phases

# COMMAND ----------

packages = pip_list(format="pyproject")
print(packages)

# COMMAND ----------

print(ROOT)

# COMMAND ----------

run_notebook(
    ROOT.joinpath("initialize"),
    expected="True",
    i=1,
)

# COMMAND ----------

sys.dont_write_bytecode = True

# COMMAND ----------

DEFAULT_LOGGER.setLevel(ERROR)

# COMMAND ----------

k = " or ".join(phases)

# COMMAND ----------

res = pytest.main(
    [
        "phases",
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
