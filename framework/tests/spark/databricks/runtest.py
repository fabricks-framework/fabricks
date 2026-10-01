# Databricks notebook source
# Ad-hoc pytest runner on the interactive cluster, for fast iteration without a full runtests.py pass.
# runjobs.py is the equivalent for fabricks jobs.

import logging
import os
import sys

sys.dont_write_bytecode = True

from databricks.sdk.runtime import dbutils  # noqa: E402
import pytest  # noqa: E402

from fabricks.context import CATALOG, IS_UNITY_CATALOG, SPARK  # noqa: E402
from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("test_path", "test_scratch.py", label="Test path(s) (relative to this folder, ';'-separated)")
test_paths = [p.strip() for p in dbutils.widgets.get("test_path").split(";") if p.strip()]

dbutils.widgets.dropdown("run_fixtures", "False", ["True", "False"], label="Run notebook-deploy + schedule fixtures")
if dbutils.widgets.get("run_fixtures").lower() != "true":
    # conftest.py's session-scoped autouse fixtures would otherwise fire for every test, however self-contained.
    os.environ["FABRICKS_SKIP_FIXTURES"] = "true"

# COMMAND ----------

# Same live-catalog guard as runtests.py: a destructive scratch test on the wrong catalog is unrecoverable.
if IS_UNITY_CATALOG:
    current_catalog = SPARK.catalog.currentCatalog()
    assert current_catalog == "bms_dna_test", (
        f"unity catalog run must target bms_dna_test, but spark is on {current_catalog!r} (config: {CATALOG!r})"
    )

# COMMAND ----------

# Silence fabricks logs; pytest output and _FailureCollector's summary are the signal.
DEFAULT_LOGGER.setLevel("CRITICAL")
logging.getLogger("dags").setLevel("CRITICAL")

# COMMAND ----------

# Same as runtests.py: the Jobs API doesn't surface a notebook task's stdout, so failures go in the AssertionError.


class _FailureCollector:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.failed and report.when != "teardown":
            lines = report.longreprtext.splitlines()
            detail = next((line for line in lines if line.startswith("E ")), lines[-1] if lines else "")
            self.failures.append(f"{report.nodeid}: {detail}")

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed:
            lines = report.longreprtext.splitlines()
            detail = next((line for line in lines if line.startswith("E ")), lines[-1] if lines else "")
            self.failures.append(f"{report.nodeid} (collection): {detail}")


# COMMAND ----------

collector = _FailureCollector()
res = pytest.main([*test_paths, "-vv", "--tb=long", "-s", "-p", "no:cacheprovider"], plugins=[collector])
if res != 0:
    summary = "\n".join(collector.failures) or "(no per-test failure detail collected)"
    raise AssertionError(f"test(s) failed:\n{summary}")

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
