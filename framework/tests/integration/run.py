# Databricks notebook source
# MAGIC %run ./add_fabricks

# COMMAND ----------

import logging

from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER, LOGLEVEL
from fabricks.core import get_job
from fabricks.utils.helpers import run_in_parallel
from tests.integration.utils import landing_to_raw

# COMMAND ----------

dbutils.widgets.dropdown("i", "1", ["1", "2", "3"])

# COMMAND ----------

i = dbutils.widgets.get("i")
i = int(i)

# COMMAND ----------

bronze = [
    # {"step": "bronze", "topic": "monarch", "item": "scd1"},
    # {"step": "bronze", "topic": "monarch", "item": "scd2"},
    {"step": "bronze", "topic": "king", "item": "scd1"},
    {"step": "bronze", "topic": "king", "item": "scd2"},
    {"step": "bronze", "topic": "memory", "item": "scd1"},
    {"step": "bronze", "topic": "memory", "item": "scd2"},
    {"step": "bronze", "topic": "queen", "item": "scd1"},
    {"step": "bronze", "topic": "queen", "item": "scd2"},
]

silver = [
    {"step": "silver", "topic": "regent", "item": "scd1"},
    {"step": "silver", "topic": "regent", "item": "scd2"},
    {"step": "silver", "topic": "monarch", "item": "scd1"},
    {"step": "silver", "topic": "monarch", "item": "scd2"},
    {"step": "silver", "topic": "memory", "item": "scd1"},
    {"step": "silver", "topic": "memory", "item": "scd2"},
    {"step": "silver", "topic": "king_and_queen", "item": "scd1"},
    {"step": "silver", "topic": "king_and_queen", "item": "scd2"},
]

gold = [
    {"step": "gold", "topic": "scd1", "item": "memory"},
    {"step": "gold", "topic": "scd2", "item": "memory"},
    {"step": "gold", "topic": "scd1", "item": "complete"},
    {"step": "gold", "topic": "scd2", "item": "complete"},
    {"step": "gold", "topic": "scd1", "item": "update"},
    {"step": "gold", "topic": "scd2", "item": "update"},
    {"step": "gold", "topic": "scd1", "item": "identity"},
]

# COMMAND ----------

if i == 2:
    silver = silver + [
        {"step": "silver", "topic": "prince", "item": "deletelog"},
        {"step": "silver", "topic": "princess", "item": "append"},
        {"step": "silver", "topic": "princess", "item": "latest"},
        {"step": "silver", "topic": "princess", "item": "schema_drift"},
        {"step": "silver", "topic": "princess", "item": "check"},
    ]

    gold = gold + [
        {"step": "semantic", "topic": "fact", "item": "schema_drift"},
        {"step": "gold", "topic": "scd1", "item": "last_timestamp"},
    ]

# COMMAND ----------

errors = []

# COMMAND ----------


def _run(job: dict):
    j = get_job(step=job.get("step"), topic=job.get("topic"), item=job.get("item"))  # type: ignore
    try:
        j.run()
    except Exception as e:
        errors.append((job, e))


# COMMAND ----------

landing_to_raw(i)

# COMMAND ----------

DEFAULT_LOGGER.setLevel(logging.CRITICAL)

# COMMAND ----------

_ = run_in_parallel(_run, bronze, progress_bar=True)
_ = run_in_parallel(_run, silver, progress_bar=True)
_ = run_in_parallel(_run, gold, progress_bar=True)

# COMMAND ----------

DEFAULT_LOGGER.setLevel(LOGLEVEL)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
