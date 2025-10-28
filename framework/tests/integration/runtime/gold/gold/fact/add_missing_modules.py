# Databricks notebook source
import os
import sys
from pathlib import Path

# COMMAND ----------

# https://docs.databricks.com/aws/en/files/workspace-modules

# COMMAND ----------

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

# COMMAND ----------

root = p.absolute()

# COMMAND ----------

if str(root) not in sys.path:
    print(f"adding {root} to sys.path")
    sys.path.insert(0, str(root))
