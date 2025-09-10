# Job Helper

This helper describes a widget-driven approach to manage a Fabricks job.

Core imports used:
```python
from fabricks.api import get_job
```

## Typical usage

1) Resolve the job:
```python
from fabricks.api import get_job

j = get_job(job="gold.sales.orders")  # or pass step/topic/item explicitly
```

2) Run actions:
```python
# Examples:
j.run()

# Other common operations:
j.update_schema()
j.overwrite_schema()
j.register()

j.create()

# Cleanup if required:
# j.truncate()
# j.drop()
```

3) Gold jobs:
```python
# If your job is gold, ensure UDFs are registered before actions:
if getattr(j, "expand", None) == "gold":
    j.register_udfs()
j.run()
```

## Helper Notebook

```python
# Databricks notebook source
import os
from multiprocessing import Pool
from typing import Callable, List

from databricks.sdk.runtime import dbutils, spark

# COMMAND ----------

dbutils.widgets.text("jobs", "---", label="6 - Job(s)")
dbutils.widgets.multiselect(
    "actions",
    "run",
    [
        "drop",                   -- Drop the job
        "register",               -- Register the table
        "create",                 -- Create the job
        "truncate",               -- Truncate the table
        "run",                    -- Run the job 
        "for-each-run",           -- Run the job (excluding the invoker(s) and the check(s))
        "overwrite",              -- Overwrite the schema and run the job
        "pre-run-invoke",         -- Run the pre-run invoker(s)
        "pre-run-check",          -- Run the pre-run check(s)
        "post-run-invoke",        -- Run the post-run invoker(s)
        "post-run-check",         -- Run the post-run check(s)
        "update-schema",          -- Update the schema
        "overwrite-schema",       -- Overwrite the schema
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

from fabricks.api import get_job  # noqa: E402
from fabricks.api.log import DEFAULT_LOGGER  # noqa: E402
from fabricks.core.jobs import Gold  # noqa: E402

# COMMAND ----------

actions = dbutils.widgets.get("actions").split(",")
actions = [a.strip() for a in actions]

# COMMAND ----------


def do(job: str) -> None:
    j = get_job(job=job)
    todos: dict[str, Callable] = {}

    if j.expand == "gold":
        assert isinstance(j, Gold)
        j.register_udfs()

    if "drop" in actions:
        todos["drop"] = j.drop

    if "register" in actions:
        todos["register"] = j.register

    if "pre-run-invoke" in actions:
        todos["pre-run-invoke"] = j.invoke_pre_run

    if "pre-run-check" in actions:
        todos["pre-run-check"] = j.check_pre_run

    if "create" in actions:
        todos["create"] = j.create

    if "update-schema" in actions:
        todos["update-schema"] = j.update_schema

    if "overwrite-schema" in actions:
        todos["overwrite-schema"] = j.overwrite_schema

    if "truncate" in actions:
        todos["truncate"] = j.truncate

    if "run" in actions:
        todos["run"] = j.run

    if "for-each-run" in actions:
        todos["for-each-run"] = j.for_each_run

    if "overwrite" in actions:
        todos["overwrite"] = j.overwrite

    if "post-run-check" in actions:
        todos["post-run-check"] = j.check_post_run

    if "post-run-invoke" in actions:
        todos["post-run-invoke"] = j.invoke_post_run

    for key, func in todos.items():
        func()

# COMMAND ----------

actions = [s.strip() for s in dbutils.widgets.get("actions").split(",")]
jobs = [s.strip() for s in dbutils.widgets.get("jobs").split(",")]
jobs = [[j.strip() for j in job.split("//")] if "//" in job else job for job in jobs]

for job in jobs:
    DEFAULT_LOGGER.warning(", ".join(actions), extra={"job": job})

# COMMAND ----------

for job in jobs:
    if isinstance(job, List):
        with Pool() as pool:
            results = pool.map(do, job)
    else:
        do(job)

# COMMAND ----------

dbutils.notebook.exit("exit (0)")  # type: ignore
```

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Runtime overview and sample runtime: [Runtime](../helpers/runtime.md)
- Checks & Data Quality: [Checks and Data Quality](../reference/checks-data-quality.md)
- Table options and storage layout: [Table Options](../reference/table-options.md)
- Extenders, UDFs & Views: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
