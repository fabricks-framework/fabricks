# Databricks notebook source
# Ad-hoc single-test runner on the persistent interactive cluster, for fast
# iteration during manual debugging -- e.g. checking a real physical plan
# (Photon operator naming, join strategy) without paying for a full
# runtests.py pass (seed + armageddon + the whole tests/spark/databricks
# suite, ~20-25 min). See runjobs.py for the equivalent ad-hoc runner for
# fabricks jobs (bronze/silver/gold), rather than pytest test files.

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
    # conftest.py's _notebooks_deployed/_schedule_run fixtures are
    # session-scoped autouse -- they'd otherwise fire for any test in this
    # directory regardless of what it actually needs, defeating the point
    # of a fast scoped run for a self-contained scratch test.
    os.environ["FABRICKS_SKIP_FIXTURES"] = "true"

# COMMAND ----------

# same guard as runtests.py: assert the *live* session catalog, not just
# CATALOG from the config -- a scratch test doing something destructive
# against the wrong catalog is unrecoverable.
if IS_UNITY_CATALOG:
    current_catalog = SPARK.catalog.currentCatalog()
    assert current_catalog == "bms_dna_test", (
        f"unity catalog run must target bms_dna_test, but spark is on {current_catalog!r} (config: {CATALOG!r})"
    )

# COMMAND ----------

# fabricks' own DEFAULT_LOGGER output is noise here -- pytest's own -vv/-s
# output plus _FailureCollector's summary are the useful signal.
DEFAULT_LOGGER.setLevel("CRITICAL")
logging.getLogger("dags").setLevel("CRITICAL")

# COMMAND ----------

# same _FailureCollector pattern as runtests.py: puts per-test failure
# detail into the raised AssertionError itself, since the Jobs API doesn't
# surface a notebook task's plain stdout on failure.


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
