"""Conftest for the unit/config tier: real fabricks.context/get_step/
get_job against real YAML, with Spark faked out so no JVM (and therefore no
Java installation) is needed. See docs/superpowers/plans/
2026-09-03-local-get-job-get-step.md, Task 4, for the design (written when
this tier still lived at tests/spark/config/ - see docs/TEST.md for the
current tests/unit/config/ path).

Three independent real-Spark/Databricks-construction paths have to be
defused, not one:

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
   sys.modules first, the same seam tests/unit/plain/conftest.py uses -
   but WITHOUT also replacing fabricks.context itself, since this tier
   wants fabricks.context's real STEPS/CONF_RUNTIME parsing to run.
3. fabricks/core/schedules/dags.py does `from databricks.sdk.runtime import
   dbutils, spark` at ITS OWN import time (reached transitively via
   fabricks.api -> fabricks.api.deploy -> fabricks.deploy ->
   fabricks.deploy.schedules -> fabricks.core.schedules ->
   fabricks.core.schedules.dags). Outside a real Databricks cluster,
   databricks-sdk's runtime shim tries to fall back to a real
   WorkspaceClient/default-credentials auth and raises ValueError. Every
   other `databricks.sdk.runtime` import in fabricks/ is inside a function
   body (lazy, only triggered by a call), so dags.py's module-level import
   is the one seam that needs defusing here too.
4. fabricks/core/dags/log.py does `table = get_table()` at ITS OWN import
   time (reached via the same fabricks.api chain, one hop further:
   fabricks.core.dags.generator/base/processor/run/terminator all import
   LOGGER/TABLE_LOG_HANDLER from it), and get_table() calls
   `FABRICKS_STORAGE.get_storage_account()` - only implemented on the real
   Azure FileSharePath, not the LocalFileSharePath this docker/local
   environment uses. Only LOGGER/TABLE_LOG_HANDLER are ever consumed from
   this module, so it's faked wholesale too.

IMPORTANT: same import-order rule as tests/spark/apache/conftest.py - the
env vars and both patches below must execute before fabricks.context is
ever imported (by this conftest or by any test file), since
fabricks.context.SPARK is built once, at that first import, and cached at
module level.

Do NOT mix tests/unit/config with tests/unit/plain (or tests/spark/apache,
tests/spark/databricks) in the same pytest invocation, for the same
reason: tests/unit/plain/conftest.py replaces sys.modules["fabricks.context"]
with a MagicMock outright, and whichever conftest's module body runs first
wins for the whole process - this tier needs the real fabricks.context,
tests/unit/plain needs the fake one. Confirmed empirically:
`pytest tests/unit/plain tests/unit/config` in one run fails collecting
tests/unit/config with `ModuleNotFoundError: fabricks.context is not a
package`, because tests/unit/plain's mock (collected first) already
replaced it. Run each tier as its own separate pytest invocation.
"""

import os
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/apache/runtime"
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

sys.modules["databricks.sdk.runtime"] = MagicMock(
    name="fake_databricks_sdk_runtime", spark=_fake_spark_session, dbutils=_fake_dbutils
)

sys.modules["fabricks.core.dags.log"] = MagicMock(
    name="fake_dags_log",
    LOGGER=MagicMock(name="fake_dags_logger"),
    TABLE_LOG_HANDLER=MagicMock(name="fake_table_log_handler"),
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
