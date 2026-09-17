# Databricks notebook source
# MAGIC %md
# MAGIC ### Code

# COMMAND ----------

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
import re

from databricks.sdk.runtime import dbutils, spark
from tqdm import tqdm

# COMMAND ----------
from fabricks.api import get_job
from fabricks.api.log import DEFAULT_LOGGER
from fabricks.context import LOGLEVEL
from fabricks.core.jobs import Gold

# COMMAND ----------

Actions = [
    "~",
    "drop",
    "register",
    "create",
    "truncate",
    "run",
    "optimize",
    "compute_statistics",
    "vacuum",
    "for-each-run",
    "overwrite",
    "pre-run-invoke",
    "pre-run-check",
    "post-run-invoke",
    "post-run-check",
    "update-schema",
    "update-dependencies",
    "overwrite-schema",
]
Booleans = ["~", "True", "False"]

# COMMAND ----------

dbutils.widgets.text("workers", "~", label="1 - Workers")
dbutils.widgets.multiselect("actions", "~", Actions, label="2 - Action(s)")
dbutils.widgets.dropdown("stop_on_fail", "~", Booleans, label="3 - Stop on Fail")
dbutils.widgets.text("jobs", "---", label="4 - Job(s)")

# COMMAND ----------

workers = int(dbutils.widgets.get("workers")) if dbutils.widgets.get("workers") != "~" else 4
stop_on_fail = dbutils.widgets.get("stop_on_fail") == "True" if dbutils.widgets.get("stop_on_fail") != "~" else True

# COMMAND ----------

actions = dbutils.widgets.get("actions").split(",")
actions = [a.strip() for a in actions]

# COMMAND ----------

def do(job: str) -> tuple:
    todos: dict[str, Callable] = {}

    try:
        j = get_job(job=job)

        if "~" in actions:
            todos["overwrite-schema"] = j.overwrite_schema
            todos["run"] = j.run

        else:
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

            if "update-dependencies" in actions:
                todos["update-dependencies"] = j.update_dependencies

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

            optimize = "optimize" in actions
            vacuum = "vacuum" in actions
            compute_statistics = "compute_statistics" in actions

            if optimize or vacuum or compute_statistics:
                todos["optimize"] = j.maintain

        if j.expand == "gold":
            assert isinstance(j, Gold)
            j.register_udfs()

        for key, func in todos.items():
            if key == "optimize":
                func(
                    compute_statistics=compute_statistics,
                    vacuum=vacuum,
                    optimize=optimize,
                )
            else:
                func()

    except Exception as e:
        if stop_on_fail:
            raise
        return job, False, e

    return job, True

# COMMAND ----------


def parse_complex_list(input: str) -> list[str]:
    if " * " in input:
        prefix, input = input.split(" * ", 1)
        prefix = prefix.strip()
    else:
        prefix = None

    if "," not in input and " " not in input:
        items = [input]
    else:
        items = []
        pos = 0

        def parse_segment(segment: str) -> list[str]:
            if "," in segment:
                return [item.strip() for item in segment.split(",") if item.strip()]
            return [item.strip() for item in segment.split() if item.strip()]

        bracket_pattern = re.compile(r"\[(.*?)\]")
        for match in bracket_pattern.finditer(input):
            if pos < match.start():
                segment = input[pos : match.start()]
                items.extend(parse_segment(segment))

            nested_content = match.group(1)
            nested_items = [item.strip() for item in nested_content.split(",")]
            items.append(nested_items)

            pos = match.end()

        if pos < len(input):
            items.extend(parse_segment(input[pos:]))

    if prefix:
        result = []
        for item in items:
            if isinstance(item, list):
                result.append([f"{prefix}{j}" for j in item])
            else:
                result.append(f"{prefix}{item}")
        return result

    return items

# COMMAND ----------

actions = [s.strip() for s in dbutils.widgets.get("actions").split(",")]
jobs = parse_complex_list(dbutils.widgets.get("jobs"))

iterator = {}
for i, job in enumerate(jobs):
    if isinstance(job, list):
        iterator[i] = [j.strip() for j in job]
    else:
        iterator[i] = [job.strip()]

# COMMAND ----------

for i in iterator:
    for job in iterator[i]:
        DEFAULT_LOGGER.info(", ".join(actions), extra={"job": job})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run

# COMMAND ----------

spark.sql("SET spark.sql.ansi.enabled = FALSE;")

# COMMAND ----------

results = []
for i in iterator:
    if len(iterator[i]) > 1:
        with ThreadPoolExecutor(max_workers=workers) as e:
            DEFAULT_LOGGER.setLevel("CRITICAL")
            results += list(tqdm(e.map(do, iterator[i]), total=len(iterator[i])))
            DEFAULT_LOGGER.setLevel(LOGLEVEL)
    else:
        results += [do(iterator[i][0])]

# COMMAND ----------

failed = [r for r in results if not r[1]]
for f in failed:
    DEFAULT_LOGGER.exception("failed", extra={"job": f[0]}, exc_info=f[2])

# COMMAND ----------

if failed:
    raise ValueError(f"{len(failed)} job(s) failed")

# COMMAND ----------

dbutils.notebook.exit("🛑")  # type: ignore

