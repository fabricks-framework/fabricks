# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.deploy import Deploy
from tests.integration._types import steps

# COMMAND ----------

from fabricks.context import FABRICKS_STORAGE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base._types import Steps, TStep
from fabricks.core.schedules import create_or_replace_views as create_or_replace_schedules_views
from fabricks.core.steps.base import BaseStep
from fabricks.core.views import create_or_replace_views as create_or_replace_custom_views
from fabricks.deploy.masks import deploy_masks
from fabricks.deploy.notebooks import deploy_notebooks
from fabricks.deploy.schedules import deploy_schedules
from fabricks.deploy.tables import deploy_tables
from fabricks.deploy.udfs import deploy_udfs
from fabricks.deploy.utils import print_atomic_bomb
from fabricks.deploy.views import deploy_views
from fabricks.metastore.database import Database

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

Deploy.armageddon(steps=steps, nowait=True)  # why wait ?

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
