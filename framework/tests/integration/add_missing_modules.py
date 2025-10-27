# Databricks notebook source
import os
import sys
from pathlib import Path

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
    print(f"adding {root / 'tests'} to sys.path")
    sys.path.insert(0, str(root / "tests"))
