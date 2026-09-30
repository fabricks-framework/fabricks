"""Conftest for the unit/config tier: real fabricks.context/get_step/
get_job against real YAML, with Spark faked out so no JVM (and therefore no
Java installation) is needed. See docs/superpowers/plans/
2026-09-03-local-get-job-get-step.md, Task 4, for the design (written when
this tier still lived at tests/spark/config/ - see docs/TEST.md for the
current tests/unit/config/ path).

Two independent real-Spark construction paths have to be defused:

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

`databricks.sdk.runtime` is still replaced in sys.modules below, as a tripwire: nothing imports it at
module level any more (Step 0 seam), but a stray real import would try to authenticate against a
workspace. Per-test runtime fakes come from the `semblance` fixture (tests/semblance/).
`fabricks.core.dags.log` is NOT faked: its table is resolved lazily, so it imports safely.

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
import time
from unittest.mock import MagicMock

import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

from tests.tier_policy import activate_tier  # noqa: E402

activate_tier("config")

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/runtime"
os.environ["FABRICKS_CONFIG"] = "tests/spark/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_DEBUGMODE"] = "FALSE"
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

from pyspark.sql import SparkSession  # noqa: E402 - must follow the setup above

_fake_builder = MagicMock(name="fake_spark_session_builder")
_fake_builder.appName.return_value = _fake_builder
_fake_builder.config.return_value = _fake_builder
_fake_builder.enableHiveSupport.return_value = _fake_builder
_fake_builder.getOrCreate.return_value = _fake_spark_session
SparkSession.builder = _fake_builder


@pytest.fixture
def no_real_sleep(monkeypatch):
    """Skip a real tenacity retry wait (e.g. invoker.py's wait_fixed(60))."""
    monkeypatch.setattr(time, "sleep", lambda *_a, **_kw: None)


@pytest.fixture(autouse=True)
def _fake_dags_log_table(monkeypatch):
    """Give the real dags log handler a fake table so nothing resolves the lazy one (local storage has no
    get_storage_account) and drain what the real LOGGER buffered, so logging.shutdown() has nothing to flush.

    dags.log is imported here, not looked up in sys.modules, so the seam does not depend on which test
    modules were collected (some import the DAG code only inside the test body)."""
    from fabricks.core.dags.log import TABLE_LOG_HANDLER  # this tier uses the real fabricks.context: safe

    monkeypatch.setattr(TABLE_LOG_HANDLER, "_table", MagicMock(name="fake_dags_log_table"))
    yield
    TABLE_LOG_HANDLER.clear_buffer()
