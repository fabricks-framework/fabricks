# Databricks notebook source
import time

from databricks.sdk.runtime import dbutils

# COMMAND ----------

time.sleep(20)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
