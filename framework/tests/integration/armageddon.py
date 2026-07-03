# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------


from fabricks.deploy import Deploy
from tests.integration._types import STEPS

# COMMAND ----------

Deploy.armageddon(steps=STEPS, nowait=True, mode="parallel", deploy_notebooks=False)  # why wait ?
