# Step Helper (Databricks Notebook)

This helper describes a widget-driven approach in a Databricks notebook to manage a Fabricks step (bronze, silver, gold). 

Core imports used:
```python
from fabricks.api import get_step
```

## Typical usage

1) Resolve the step:
```python
from fabricks.api import get_step

s = get_step("gold")  # or "bronze"/"silver"
```

2) Run actions:
```python
# Examples:
s.update_jobs()
s.update_dependencies(progress_bar=False)

# Cleanup if required:
# s.drop()
```

## Helper Notebook

``` python
# Databricks notebook source
import os
from multiprocessing import Pool
from typing import Callable, List

from databricks.sdk.runtime import dbutils, spark

# COMMAND ----------

dbutils.widgets.text("steps", "---", label="6 - Step(s)")
dbutils.widgets.multiselect(
    "actions",
    "update-configurations",
    [
        "update-configurations",    -- Update job configurations
        "add-missing-jobs",         -- Create any missing jobs
        "update-dependencies",      -- Update job dependencies
        "update-lists",             -- Update lists of jobs/tables/views
        "update-tables-list",       -- Update the list of tables
        "update-views-list",        -- Update the list of views
        "update"                    -- Update the step
    ],
    label="5 - Action(s)"
)
dbutils.widgets.dropdown("loglevel", "info", ["debug", "info", "error", "critical"], label="3 - Log Level")
dbutils.widgets.dropdown("chdir", "False", ["True", "False"], label="1 - Change Directory")
dbutils.widgets.dropdown("config_from_yaml", "True", ["True", "False"], label="2 - Use YAML")
dbutils.widgets.dropdown("debugmode", "False", ["True", "False"], label="4 - Debug Mode")

# COMMAND ----------

if dbutils.widgets.get("chdir").lower() == "true":
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()  # type: ignore
    try:
        os.chdir(f"/Workspace/Users/{user}/Fabricks.Runtime")
    except FileNotFoundError:
        os.chdir(f"/Workspace/Users/{user}/runtime")

# COMMAND ----------

if dbutils.widgets.get("config_from_yaml").lower() == "true":
    os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "1"
else:
    os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "0"

# COMMAND ----------

if dbutils.widgets.get("debugmode").lower() == "true":
    os.environ["FABRICKS_IS_DEBUGMODE"] = "1"
else:
    os.environ["FABRICKS_IS_DEBUGMODE"] = "0"

# COMMAND ----------

loglevel = dbutils.widgets.get("loglevel")
if loglevel == "debug":
    os.environ["FABRICKS_LOGLEVEL"] = "DEBUG"
elif loglevel == "info":
    os.environ["FABRICKS_LOGLEVEL"] = "INFO"
elif loglevel == "error":
    os.environ["FABRICKS_LOGLEVEL"] = "ERROR"
elif loglevel == "critical":
    os.environ["FABRICKS_LOGLEVEL"] = "CRITICAL"

# COMMAND ----------

from fabricks.api import get_step  # noqa: E402
from fabricks.api.log import DEFAULT_LOGGER  # noqa: E402

# COMMAND ----------

actions = dbutils.widgets.get("actions").split(",")
actions = [a.strip() for a in actions]

# COMMAND ----------


def do(step: str) -> None:
    s = get_step(step=step)
    todos: dict[str, Callable] = {}

    if "update-configurations" in actions:
        todos["update-configurations"] = s.update_jobs

    if "add-missing-jobs" in actions:
        todos["add-missing-jobs"] = s.create_jobs

    if "update-views-list" in actions or "update-lists" in actions:
        todos["update-views-list"] = s.update_views

    if "update-tables-list" in actions or "update-lists" in actions:
        todos["update-tables-list"] = s.update_tables

    if "update" in actions and len(actions) == 1:
        todos["update"] = s.update

    if "update-dependencies" in actions:
        todos["update-dependencies"] = s.update_dependencies

    for key, func in todos.items():
        func()

# COMMAND ----------

actions = [s.strip() for s in dbutils.widgets.get("actions").split(",")]
steps = [s.strip() for s in dbutils.widgets.get("steps").split(",")]
steps = [[j.strip() for j in job.split("//")] if "//" in job else job for job in steps]

for step in steps:
    DEFAULT_LOGGER.warning(", ".join(actions), extra={"step": step})

# COMMAND ----------

for step in steps:
    if isinstance(step, List):
        with Pool() as pool:
            results = pool.map(do, step)
    else:
        do(step)

# COMMAND ----------

dbutils.notebook.exit("exit (0)")  # type: ignore

# COMMAND ----------
```

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Runtime overview and sample runtime: [Runtime](../helpers/runtime.md)
- Checks & Data Quality: [Checks and Data Quality](../reference/checks-data-quality.md)
- Table options and storage layout: [Table Options](../reference/table-options.md)
- Extenders, UDFs & Views: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
