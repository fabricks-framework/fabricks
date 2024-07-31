# Databricks notebook source
# MAGIC %pip install python-dotenv

# COMMAND ----------

import os
import subprocess
from pathlib import Path

from databricks.sdk.runtime import dbutils
from dotenv import load_dotenv

# COMMAND ----------

load_dotenv()

# COMMAND ----------

try:
    dbutils.fs.ls("mnt/fabricks")
except Exception:
    print("fabricks container not mounted")

# COMMAND ----------

try:
    dbutils.fs.ls("mnt/fabricks/versions")
except Exception:
    print("fabricks not found")

# COMMAND ----------

runtime = os.environ.get("path_runtime")
assert runtime
if not Path(runtime).exists():
    print("runtime not found")

# COMMAND ----------

notebooks = os.environ.get("path_notebooks")
assert notebooks
if not Path(notebooks).exists():
    print("notebooks not found")

# COMMAND ----------

scripts = os.environ.get("path_scripts")
assert scripts
if not Path(scripts).exists():
    print("scripts not found")

# COMMAND ----------

abfss_wheels = "abfss://fabricks-wheels@bmsstaprdeuwsoftware.dfs.core.windows.net"

# COMMAND ----------

version = os.environ.get("fabricks_version")

# COMMAND ----------

mnt_version = f"dbfs:/mnt/fabricks/versions/{version}"
fuse_mnt_version = f"/dbfs/mnt/fabricks/versions/{version}"

# COMMAND ----------

try:
    for f in dbutils.fs.ls(mnt_version):
        dbutils.fs.rm(f.path, True)
except Exception:
    pass

dbutils.fs.rm(mnt_version, True)
dbutils.fs.mkdirs(mnt_version)

# COMMAND ----------

dbutils.fs.rm(mnt_version, True)
dbutils.fs.mkdirs(mnt_version)

# COMMAND ----------

print("copying version to", f"{mnt_version}/version")

for f in dbutils.fs.ls(f"{abfss_wheels}/{version}"):
    to = f"{mnt_version}/{f.name}"

    try:
        dbutils.fs.ls(to)
    except Exception:
        print("uploading", f.name)
        dbutils.fs.cp(f.path, to, recurse=True)

# COMMAND ----------

print("pip install requirements.txt")

out = subprocess.run(
    [
        "pip",
        "install",
        "--no-index",
        f"--find-links={fuse_mnt_version}/wheels",
        "-r",
        f"{fuse_mnt_version}/requirements.txt",
    ],
    capture_output=True,
)

if out.returncode == 1:
    raise ValueError(out.stderr)

# COMMAND ----------

latest = os.environ.get("latest")
assert latest
latest = latest.lower() == "true"

# COMMAND ----------

versions = [version, "2023.12.*"] if latest else [version]

# COMMAND ----------

print("deploy init script")

for v in versions:
    path = f"{scripts}/{v}.sh"

    with open(path, "w") as sh:
        sh.write(
            f"""
            sudo echo FABRICKS_RUNTIME={runtime} >> /etc/environment
            sudo echo FABRICKS_NOTEBOOKS={notebooks}/{version} >> /etc/environment
            sudo echo FABRICKS_VERSION={version} >> /etc/environment

            /databricks/python/bin/pip install --no-index --find-links='{fuse_mnt_version}/wheels' -r '{fuse_mnt_version}/requirements.txt'
            /databricks/python/bin/pip install sqlglot
            /databricks/python/bin/pip install jinja2
            """.replace("            ", "").strip()
        )

# COMMAND ----------

dbutils.notebook.exit("exit (0)")

# COMMAND ----------
