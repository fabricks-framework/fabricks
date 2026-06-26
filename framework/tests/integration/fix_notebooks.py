# Databricks notebook source
# MAGIC %run ./add_missing_modules

# COMMAND ----------

from pathlib import Path

from databricks.sdk.runtime import dbutils

import fabricks.api.notebooks as _nb_pkg
from fabricks.context import PATH_NOTEBOOKS

# COMMAND ----------

src_dir = Path(_nb_pkg.__file__).parent
dest_dir = Path(str(PATH_NOTEBOOKS))

for name in ["initialize", "process", "standalone", "run", "terminate"]:
    content = (src_dir / f"{name}.py").read_text()

    if "# MAGIC %run ./add_missing_modules" not in content:
        content = content.replace(
            "# Databricks notebook source\n",
            "# Databricks notebook source\n# MAGIC %run ./add_missing_modules\n# COMMAND ----------\n",
        )

    (dest_dir / f"{name}.py").write_text(content)

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
