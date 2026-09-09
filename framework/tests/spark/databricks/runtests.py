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
# king/regent/queen (the tagged bronze jobs) are all register mode now. king
# and queen read their own per-run-seeded Delta table; regent still reads an
# already-real, standing external table on the storage account (Unity
# Catalog binds a path to one table for good -- re-seeding/re-registering it
# here on every run would collide with that existing binding). bronze.
# feature_parser (untagged, real file parsing via the "dummy" parser plugin)
# is the one job that still needs raw json files seeded.

from logging import INFO

from databricks.sdk.runtime import dbutils
import pytest

from fabricks.context import PATH_RUNTIME, SPARK
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
    bronze.feature_parser reads from. ponytail: plain file copy, no
    landing/parquet conversion stage like the old databricks-old pipeline --
    the job's "dummy" parser reads it directly, so the git-checked-in
    fixtures work as-is.
    """
    job = get_job(step="bronze", topic="feature", item="parser")
    king_fixtures = PATH_RUNTIME.parent().parent().joinpath("fixtures", "iter1", "king")
    job.data_path.rm()
    for f in king_fixtures.walk(convert=True, file_format="jsonl"):
        rel = f.string.removeprefix(f"{king_fixtures.string}/")
        dbutils.fs.cp(f"file:{f.string}", job.data_path.joinpath(rel).string)


def _seed_raw_delta_fixtures() -> None:
    """Write a small Delta table at king/queen's raw uri -- Bronze.
    register_external_table() (bronze.py) selects from that uri directly, so
    it needs a real table there, same shape as tests/spark/apache/conftest.py's
    king_and_queen_registered_sources.

    regent is deliberately excluded: its raw delta table is an already-real,
    standing external table on the storage account, registered under
    bronze.regent_scd1 outside this per-run lifecycle -- overwriting it here
    would mean re-registering (or re-pointing at) a path Unity Catalog has
    already bound to that table, which UC rejects the same way it rejects
    binding any path to a second table.
    """
    from pyspark.sql.functions import col, lit

    rows = [
        {"id": 1, "name": "Leopold I", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
        {"id": 2, "name": "Leopold II", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
    ]
    for topic in ("king", "queen"):
        job = get_job(step="bronze", topic=topic, item="scd1")
        df = SPARK.createDataFrame(rows)
        df = df.withColumn("__source", lit(topic)).withColumn("__timestamp", col("__timestamp").cast("timestamp"))
        df.write.format("delta").mode("overwrite").save(job.data_path.string)


if seed_raw:
    _seed_raw_fixtures()
    _seed_raw_delta_fixtures()

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
