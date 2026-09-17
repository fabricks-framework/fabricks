# Databricks notebook source

# Sets up runtime (armageddon + raw fixture data), runs the schedule, then
# the Databricks integration tests (test_schedule.py and friends) -- see
# runtime/README.md and docs/superpowers/plans/2026-09-04-databricks-cut-list.md.
#
# The "expected" database in conf.uc.fabricks.yml is scaffolding for parity
# with production config shape only -- this suite asserts against
# fabricks.last_schedule/last_status, not expected.*, so no expected views
# are created here (that comparison lives in tests/spark/apache/expected/).
#
# king/queen (the tagged bronze jobs) are both register mode, each reading
# its own per-run-seeded Delta table. bronze.feature_parser (untagged, real
# file parsing via the "dummy" parser plugin) is the one job that still
# needs raw json files seeded.

import logging
from logging import INFO
import sys

# tests/ lives on the Databricks workspace-files FUSE mount, which doesn't
# support the filesystem ops CPython needs to write __pycache__ (OSError
# [Errno 95] Operation not supported) -- disable bytecode caching entirely,
# before anything under tests/ gets imported, rather than import-erroring
# on the first conftest.py collected.
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

# Checked before armageddon drops anything: assert the *live* session catalog,
# not just CATALOG from the config -- the config only proves what
# add_catalog_to_spark asked for, and dropping against the wrong catalog is
# unrecoverable.
if IS_UNITY_CATALOG:
    current_catalog = SPARK.catalog.currentCatalog()
    assert current_catalog == "bms_dna_test", (
        f"unity catalog run must target bms_dna_test, but spark is on {current_catalog!r} (config: {CATALOG!r})"
    )

# COMMAND ----------


if seed:
    # Sibling-module import, not `tests.spark.*` -- this notebook runs from
    # bundle-synced workspace files, not a Databricks Repo, so `tests`
    # itself isn't importable here. Databricks does add a notebook's own
    # containing folder to sys.path, so this works.
    from fixtures import seed_raw_delta_fixtures, seed_raw_fixtures

    seed_raw_fixtures()
    seed_raw_delta_fixtures()

# COMMAND ----------

if armageddon:
    from fabricks.deploy import Deploy

    Deploy.armageddon(nowait=True)

# COMMAND ----------

# test_schedule.py's session fixture calls fabricks.core.schedules.standalone(schedule="test")
# itself -- running it here too would just replay the same schedule twice, so
# "call the schedule" happens as part of running the tests below, not as a
# separate step.

if runtests:
    # fabricks' own DEFAULT_LOGGER.info() output (set to INFO above, for
    # seed/armageddon) is noise here -- pytest's own -vv/-s output plus
    # _FailureCollector's summary are the useful signal during the test run.
    # DagTerminator.terminate() logs each deliberately-failing job (e.g.
    # check_fail, invoke_timeout) via a separate "dags" logger, not
    # DEFAULT_LOGGER -- silence that one too.
    DEFAULT_LOGGER.setLevel("CRITICAL")
    logging.getLogger("dags").setLevel("CRITICAL")

    class _FailureCollector:
        def __init__(self) -> None:
            self.failures: list[str] = []

        def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
            if report.failed and report.when != "teardown":
                lines = report.longreprtext.splitlines()
                detail = next((line for line in lines if line.startswith("E ")), lines[-1] if lines else "")
                self.failures.append(f"{report.nodeid}: {detail}")

        def pytest_collectreport(self, report: pytest.CollectReport) -> None:
            # Collection errors (e.g. an ImportError in a test file/conftest)
            # never reach pytest_runtest_logreport -- no test ran at all, so
            # without this the summary would say "no failure detail" even
            # though pytest never got past import.
            if report.failed:
                lines = report.longreprtext.splitlines()
                detail = next((line for line in lines if line.startswith("E ")), lines[-1] if lines else "")
                self.failures.append(f"{report.nodeid} (collection): {detail}")

    # -vv/--tb=long/-s: maximum detail in the notebook's own stdout (not
    # captured by the Jobs API for notebook tasks); _FailureCollector exists
    # because of that same gap -- it puts a one-line-per-test summary into
    # the raised AssertionError itself, which the API does surface.
    collector = _FailureCollector()
    res = pytest.main([".", "-vv", "--tb=long", "-s", "-p", "no:cacheprovider"], plugins=[collector])
    if res != 0:
        summary = "\n".join(collector.failures) or "(no per-test failure detail collected)"
        raise AssertionError(f"databricks integration tests failed:\n{summary}")

# COMMAND ----------

dbutils.notebook.exit(value="exit (0)")  # type: ignore
