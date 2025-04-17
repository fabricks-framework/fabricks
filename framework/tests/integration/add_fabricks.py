# Databricks notebook source
import os
import sys
import os
from pathlib import Path
p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent 

root = p.absolute()
root

# COMMAND ----------

if str(root) not in sys.path:
    sys.path.append(str(root))
