# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from pathlib import Path

from fabricks.context import PATH_NOTEBOOKS

# COMMAND ----------

for n in ["initialize", "process", "standalone", "run", "terminate"]:
    path = Path(str(PATH_NOTEBOOKS)) / f"{n}.py"
    with path.open() as f:
        content = f.read()

    if "# MAGIC %run ./add_missing_modules" not in content:
        content = content.replace(
            "# Databricks notebook source\n",
            "# Databricks notebook source\n# MAGIC %run ./add_missing_modules\n# COMMAND ----------\n",
        )

        with path.open("w") as f:
            f.write(content)

# COMMAND ----------
