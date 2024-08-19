# Databricks notebook source
from databricks.sdk.runtime import dbutils

# COMMAND ----------

path = "/Workspace/fabricks/scripts/tests.sh"
runtime = "/Workspace/Repos/bmeurope/Fabricks/tests/src/runtime"

print("Copying init script to", path)

with open(path, "w") as sh:
    sh.write(
        f"""
        sudo echo FABRICKS_RUNTIME={runtime} >> /etc/environment
        sudo echo FABRICKS_VERSION=TEST >> /etc/environment
        sudo echo FABRICKS_IS_DEBUG=TRUE >> /etc/environment
        sudo echo FABRICKS_IS_TEST=TRUE >> /etc/environment

        /databricks/python/bin/pip install pytest
        /databricks/python/bin/pip install pytest-order
        /databricks/python/bin/pip install pyyaml
        """.replace("        ", "").strip()
    )

print("")
print(f"Add {path} as an init script for the relevant cluster(s)")
print("")

# COMMAND ----------

dbutils.notebook.exit("exit (0)")  # type: ignore
