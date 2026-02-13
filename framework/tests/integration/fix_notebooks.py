# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

import os

from fabricks.context import PATH_NOTEBOOKS

# COMMAND ----------

for n in [
    "initialize",
    "process",
    "standalone",
    "run",
    "terminate",
]:
    path = os.path.join(str(PATH_NOTEBOOKS), f"{n}.py")
    print(path)
    with open(path, "r") as f:
        content = f.read()

    if "# MAGIC %run ./add_missing_modules" not in content:
        content = content.replace(
            "# Databricks notebook source\n",
            "# Databricks notebook source\n# MAGIC %run ./add_missing_modules\n# COMMAND ----------\n",
        )

        with open(path, "w") as f:
            f.write(content)

# COMMAND ----------


