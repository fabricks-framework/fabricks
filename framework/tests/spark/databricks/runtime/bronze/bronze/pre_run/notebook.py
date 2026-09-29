# Databricks notebook source
import json

from databricks.sdk.runtime import dbutils
from pyspark.sql.functions import col, lit

from fabricks.context import SPARK
from fabricks.core import get_job

# COMMAND ----------

dbutils.widgets.text("topic", "")
dbutils.widgets.text("schedule_variables", "{}")

# COMMAND ----------

# Mirrors tests/spark/databricks/fixtures.py's _REGISTERED_DELTA_ROWS -- kept
# in sync manually since this notebook runs from bundle-synced workspace
# files, not a Databricks Repo, and can't import that test-suite module
# (Databricks puts a notebook's own containing folder on sys.path, not
# distant ancestors -- see fixtures.py's docstring for the same rationale).
_ROWS = {
    "king": (
        {"id": 1, "name": "Leopold I", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
        {"id": 2, "name": "Leopold II", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
    ),
    "queen": ({"id": 101, "name": "Louise", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},),
}

topic = dbutils.widgets.get("topic")
variables = json.loads(dbutils.widgets.get("schedule_variables"))
iter_ = variables.get("iter", 1)

job = get_job(step="bronze", topic=topic, item="scd1")
df = SPARK.createDataFrame([dict(row) for row in _ROWS[topic]])
df = df.withColumn("__source", lit(topic)).withColumn("__timestamp", col("__timestamp").cast("timestamp"))
if not iter_:
    df = df.where("1 = 2")
df.write.format("delta").mode("overwrite").save(job.data_path.string)
