# Databricks notebook source
import os
import sys
from pathlib import Path

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

root = p.absolute()

# COMMAND ----------

if str(root) not in sys.path:
    sys.path.append(str(root))
