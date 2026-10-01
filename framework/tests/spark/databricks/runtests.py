# Databricks notebook source

# The "expected" database in conf.uc.fabricks.yml is not populated: this suite asserts against
# fabricks.last_schedule/last_status instead.

import logging
from logging import INFO
import sys

# tests/ is on the workspace-files FUSE mount, which can't write __pycache__ (OSError 95); this must run
# before anything under tests/ is imported.
sys.dont_write_bytecode = True

from databricks.sdk.runtime import dbutils  # noqa: E402
import pytest  # noqa: E402

from fabricks.context import CATALOG, IS_UNITY_CATALOG, SPARK  # noqa: E402
from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402

# COMMAND ----------

DEFAULT_LOGGER.setLevel(INFO)

# COMMAND ----------

Booleans = ["True", "False"]
dbutils.widgets.dropdown("seed", "True", Booleans)
dbutils.widgets.dropdown("armageddon", "True", Booleans)
dbutils.widgets.dropdown("runtests", "True", Booleans)

seed = dbutils.widgets.get("seed").lower() == "true"
armageddon = dbutils.widgets.get("armageddon").lower() == "true"
runtests = dbutils.widgets.get("runtests").lower() == "true"

# COMMAND ----------

# Check the live session catalog, not just CATALOG: the config only shows what add_catalog_to_spark asked for,
# and armageddon on the wrong catalog is unrecoverable.
if IS_UNITY_CATALOG:
    current_catalog = SPARK.catalog.currentCatalog()
    assert current_catalog == "bms_dna_test", (
        f"unity catalog run must target bms_dna_test, but spark is on {current_catalog!r} (config: {CATALOG!r})"
    )

# COMMAND ----------


if seed:
    # Sibling import: the notebook runs from bundle-synced files, not a Databricks Repo, so `tests` isn't importable.
    from fixtures import seed_raw_delta_fixtures, seed_raw_fixtures

    seed_raw_fixtures()
    seed_raw_delta_fixtures()

# COMMAND ----------

if armageddon:
    from fabricks.deploy import Deploy

    Deploy.armageddon(nowait=True)

# COMMAND ----------

# No separate schedule step: conftest.py's _schedule_run fixture runs it, and running it here would replay it twice.

if runtests:
    # Silence fabricks logs (the "dags" logger too, which reports each deliberately failing job);
    # pytest output and _FailureCollector's summary are the signal.
    DEFAULT_LOGGER.setLevel("CRITICAL")
    logging.getLogger("dags").setLevel("CRITICAL")

    class _FailureCollector:
        def __init__(self) -> None:
            self.failures: list[str] = []

        def _detail(self, report: pytest.TestReport | pytest.CollectReport) -> str:
            # The tail, not the first "E" line: for exceptions raised several frames down, the chained causes
            # and the failing call site are not in the first line.
            lines = report.longreprtext.splitlines()
            return "\n    ".join(lines[-25:]) if lines else ""

        def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
            if report.failed and report.when != "teardown":
                self.failures.append(f"{report.nodeid}:\n    {self._detail(report)}")

        def pytest_collectreport(self, report: pytest.CollectReport) -> None:
            # Collection errors never reach pytest_runtest_logreport, so the summary would have no detail.
            if report.failed:
                self.failures.append(f"{report.nodeid} (collection):\n    {self._detail(report)}")

    # The Jobs API doesn't capture a notebook task's stdout, so failures are also put in the AssertionError.
    collector = _FailureCollector()
    res = pytest.main([".", "-vv", "--tb=long", "-s", "-p", "no:cacheprovider"], plugins=[collector])
    if res != 0:
        summary = "\n".join(collector.failures) or "(no per-test failure detail collected)"
        raise AssertionError(f"databricks integration tests failed:\n{summary}")

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
