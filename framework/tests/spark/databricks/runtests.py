# Databricks notebook source

# Sets up runtime (armageddon + raw fixture data), runs the schedule, then
# the Databricks integration tests (test_schedule.py/test_notebook.py) --
# see runtime/README.md and docs/superpowers/plans/2026-09-04-databricks-cut-list.md.
#
# The "expected" database in conf.fabricks.yml is scaffolding for parity with
# production config shape only -- this suite asserts against
# fabricks.last_schedule/last_status, not expected.*, so no expected views
# are created here (that comparison lives in tests/spark/apache/expected/).
#
# monarch (queen's raw uri) is deliberately not seeded: queen's bronze mode
# is "memory", whose create()/for_each_run() are no-ops (see bronze.py) --
# it never reads its uri, so there is nothing to seed. regent's raw delta
# table (register mode) is likewise not seeded here -- it's an already-real,
# standing external table on the storage account, not per-run fixture data.

from logging import INFO

from databricks.sdk.runtime import dbutils
import pytest

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.deploy import Deploy

DEFAULT_LOGGER.setLevel(INFO)

# COMMAND ----------

Booleans = ["True", "False"]
dbutils.widgets.dropdown("seed_raw", "True", Booleans)
dbutils.widgets.dropdown("armageddon", "True", Booleans)
dbutils.widgets.dropdown("runtests", "True", Booleans)

seed_raw = dbutils.widgets.get("seed_raw").lower() == "true"
armageddon = dbutils.widgets.get("armageddon").lower() == "true"
runtests = dbutils.widgets.get("runtests").lower() == "true"

# COMMAND ----------


def _seed_raw_fixtures() -> None:
    """Copy the checked-in iter1 king fixtures into the real raw/king path
    runtime's bronze job reads from. ponytail: plain file copy, no
    landing/parquet conversion stage like the old databricks-old pipeline --
    the job's json parser reads it directly, so the git-checked-in fixtures
    work as-is.
    """
    king = get_job(step="bronze", topic="king", item="scd1")
    king_fixtures = PATH_RUNTIME.parent().parent().joinpath("fixtures", "iter1", "king")
    king.data_path.rm()
    for f in king_fixtures.walk(convert=True, file_format="jsonl"):
        rel = f.string.removeprefix(f"{king_fixtures.string}/")
        dbutils.fs.cp(f"file:{f.string}", king.data_path.joinpath(rel).string)


if seed_raw:
    _seed_raw_fixtures()

# COMMAND ----------

if armageddon:
    Deploy.armageddon(nowait=True)

# COMMAND ----------

# test_schedule.py's session fixture calls fabricks.core.schedules.standalone(schedule="test")
# itself -- running it here too would just replay the same schedule twice, so
# "call the schedule" happens as part of running the tests below, not as a
# separate step.

if runtests:
    # -vv/--tb=long/-s: maximum detail (full assert diffs, full tracebacks,
    # unsuppressed stdout/log output) since the only way to diagnose a failure
    # here is whatever lands in this notebook run's captured output.
    res = pytest.main([".", "-vv", "--tb=long", "-s", "-p", "no:cacheprovider"])
    assert res == 0, "databricks integration tests failed"

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
