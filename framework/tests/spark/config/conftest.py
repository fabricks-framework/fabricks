"""Conftest for the spark/config tier: real fabricks.context/get_step/
get_job against real YAML, with Spark faked out so no JVM (and therefore no
Java installation) is needed. See docs/superpowers/plans/
2026-09-03-local-get-job-get-step.md, Task 4, for the design.

Two independent real-Spark-construction paths have to be defused, not one:

1. fabricks/context/spark_session.py's `SPARK = build_spark_session(...)`
   calls `pyspark.sql.SparkSession.builder...getOrCreate()` directly - so
   that classproperty is patched to a MagicMock below.
2. fabricks/utils/spark.py does `spark = get_spark()` at ITS OWN import
   time (reached transitively via fabricks.context.spark_session ->
   fabricks.context.secret -> fabricks.utils.spark), and get_spark() does
   `from delta import configure_spark_with_delta_pip` - which fails in
   this venv (delta-spark's `delta/` package here has no `__init__.py`,
   just a namespace-package stub) regardless of Java. Patching
   SparkSession.builder doesn't reach this - fabricks.utils.spark's own
   module body never gets that far. So the whole module is replaced in
   sys.modules first, the same seam tests/plain/conftest.py uses - but
   WITHOUT also replacing fabricks.context itself, since this tier wants
   fabricks.context's real STEPS/CONF_RUNTIME parsing to run.

IMPORTANT: same import-order rule as tests/spark/apache/conftest.py - the
env vars and both patches below must execute before fabricks.context is
ever imported (by this conftest or by any test file), since
fabricks.context.SPARK is built once, at that first import, and cached at
module level.

Do NOT mix tests/spark/config with tests/plain (or tests/spark/apache,
tests/spark/databricks) in the same pytest invocation, for the same
reason: tests/plain/conftest.py replaces sys.modules["fabricks.context"]
with a MagicMock outright, and whichever conftest's module body runs first
wins for the whole process - this tier needs the real fabricks.context,
tests/plain needs the fake one. Confirmed empirically:
`pytest tests/plain tests/spark/config` in one run fails collecting
tests/spark/config with `ModuleNotFoundError: fabricks.context is not a
package`, because tests/plain's mock (collected first) already replaced
it. Run each tier as its own separate pytest invocation.
"""

import os
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_CONFIG"] = "tests/spark/apache/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

_fake_spark_session = MagicMock(name="fake_spark_session")
_fake_dbutils = MagicMock(name="fake_dbutils")

sys.modules["fabricks.utils.spark"] = MagicMock(
    spark=_fake_spark_session,
    dbutils=_fake_dbutils,
    get_spark=MagicMock(return_value=_fake_spark_session),
    get_dbutils=MagicMock(return_value=_fake_dbutils),
)

from pyspark.sql import SparkSession  # noqa: E402 - must follow the setup above

_fake_builder = MagicMock(name="fake_spark_session_builder")
_fake_builder.appName.return_value = _fake_builder
_fake_builder.config.return_value = _fake_builder
_fake_builder.enableHiveSupport.return_value = _fake_builder
_fake_builder.getOrCreate.return_value = _fake_spark_session
SparkSession.builder = _fake_builder


def pytest_collection_modifyitems(items):
    """Automatically add 'config' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        if Path(item.fspath).is_relative_to(root):
            item.add_marker(pytest.mark.config)
