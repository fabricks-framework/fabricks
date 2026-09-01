# Databricks notebook source
from pathlib import Path
import sys

# COMMAND ----------

# https://docs.databricks.com/aws/en/files/workspace-modules

# COMMAND ----------

p = Path.cwd()
while not (p / "pyproject.toml").exists():
    p = p.parent

# COMMAND ----------

root = p.absolute()

# COMMAND ----------

if str(root) not in sys.path:
    print(f"adding {root} to sys.path")
    sys.path.insert(0, str(root))
