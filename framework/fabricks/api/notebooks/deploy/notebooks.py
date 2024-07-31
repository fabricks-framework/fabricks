# Databricks notebook source
# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC %pip install databricks_cli

# COMMAND ----------

import os
from importlib import resources
from pathlib import Path

from databricks.sdk.runtime import dbutils
from databricks_cli.sdk.api_client import ApiClient  # type: ignore
from databricks_cli.workspace.api import WorkspaceApi  # type: ignore
from dotenv import load_dotenv

from fabricks.api import notebooks as src

# COMMAND ----------

load_dotenv()

# COMMAND ----------

host = os.environ.get("databricks_host")
token = os.environ.get("databricks_token")
version = os.environ.get("fabricks_version")
root = os.environ.get("path_notebooks")
latest = os.environ.get("latest")
assert latest
latest = latest.lower() == "true"
runtime = os.environ.get("path_runtime")

# COMMAND ----------

notebooks = ["cluster", "initialize", "job", "log", "optimize", "run", "terminate"]

# COMMAND ----------

client = ApiClient(host=host, token=token)
workspace_api = WorkspaceApi(client)

# COMMAND ----------

versions = [version, "latest"] if latest else [version]

# COMMAND ----------

for v in versions:
    print(f"deploy {v}")

    p = f"{root}/{v}"
    if not Path(p).exists():
        os.mkdir(p)

    for n in notebooks:
        notebook = resources.files(src) / f"{n}.py"
        workspace_api.import_workspace(
            source_path=notebook,
            target_path=f"{p.replace('/Workspace', '')}/{n}.py",
            fmt="AUTO",
            language="PYTHON",
            is_overwrite=True,
        )

# COMMAND ----------

if latest:
    p = f"{runtime}/notebooks"
    for n in notebooks:
        notebook = resources.files(src) / f"{n}.py"
        workspace_api.import_workspace(
            source_path=notebook,
            target_path=f"{p.replace('/Workspace', '')}/{n}.py",
            fmt="AUTO",
            language="PYTHON",
            is_overwrite=True,
        )

# COMMAND ----------

dbutils.notebook.exit("exit (0)")

# COMMAND ----------
