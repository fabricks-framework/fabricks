# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

import os
from logging import DEBUG

from databricks.sdk.runtime import dbutils

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.deploy import Deploy
from tests.integration._types import steps

# COMMAND ----------

DEFAULT_LOGGER.setLevel(DEBUG)

# COMMAND ----------

Deploy.armageddon(steps=steps, nowait=True)  # why wait ?

# COMMAND ----------

for n in [
    "cluster",
    "initialize",
    "process",
    "standalone",
    "run",
    "terminate",
]:
    path = os.path.join(str(PATH_NOTEBOOKS), f"{n}.py")
    with open(path, "r") as f:
        content = f.read()

    if "# MAGIC %run ./add_missing_modules" not in content:
        content = content.replace(
            "# Databricks notebook source",
            "# Databricks notebook source\n# MAGIC %run ./add_missing_modules\n# COMMAND ----------",
        )

        with open(path, "w") as f:
            f.write(content)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
