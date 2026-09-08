# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

dbutils.widgets.text("arg1", "")

# COMMAND ----------

assert dbutils.widgets.get("arg1") == "1"

# COMMAND ----------

# Deliberate failure -- proves pre-run invoker failure propagates as a job
# failure (test_gold_invoke_failed_pre_run).
raise ValueError(f"arg1 {dbutils.widgets.get('arg1')}")
