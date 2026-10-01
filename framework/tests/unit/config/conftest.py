"""Conftest for the unit/config tier: real fabricks.context/get_step/get_job against real YAML, with Spark
faked out so no JVM (and no Java installation) is needed.

Two independent real-Spark construction paths are defused:

1. fabricks/context/spark_session.py builds `SPARK` via `SparkSession.builder...getOrCreate()`, so that
   classproperty is patched to a MagicMock below.
2. fabricks/utils/spark.py calls `get_spark()` at its own import time (reached via
   fabricks.context.spark_session -> fabricks.context.secret -> fabricks.utils.spark), which imports
   `delta.configure_spark_with_delta_pip` and fails in this venv regardless of Java. So the whole module is
   replaced in sys.modules first, the same seam tests/unit/plain/conftest.py uses, but WITHOUT replacing
   fabricks.context itself: this tier wants its real STEPS/CONF_RUNTIME parsing.

`databricks.sdk.runtime` is replaced in sys.modules as a safety net (a stray real import would try to
authenticate against a workspace); tests that need a `dbutils` patch the attribute on it.
`fabricks.core.dags.log` is NOT faked: its table is resolved lazily, so it imports safely.

IMPORTANT: the env vars and both patches must run before fabricks.context is first imported (by this conftest
or any test file), since fabricks.context.SPARK is built once at that import and cached. Same rule as
tests/spark/apache/conftest.py.

Never mix this tier with tests/unit/plain (or the spark tiers) in one pytest run: tests/unit/plain/conftest.py
replaces sys.modules["fabricks.context"] with a MagicMock, and whichever conftest runs first wins for the
process. See docs/TEST.md.
"""

import os
from pathlib import Path
import sys
from unittest.mock import DEFAULT, MagicMock, NonCallableMock

from pyspark.sql import SparkSession
import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

from tests.tier_policy import activate_tier  # noqa: E402

activate_tier("config")

# --- capture what the bootstrap below overwrites, so the session finalizer can restore it ---
_ENV_KEYS = (
    "FABRICKS_BASE",
    "FABRICKS_RUNTIME",
    "FABRICKS_CONFIG",
    "FABRICKS_ENVIRONMENT",
    "FABRICKS_IS_DEBUGMODE",
    "FABRICKS_IS_JOB_CONFIG_FROM_YAML",
)
_ORIGINAL_ENV = {key: os.environ.get(key) for key in _ENV_KEYS}
_REPLACED_MODULES = ("fabricks.utils.spark", "databricks.sdk.runtime")
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _REPLACED_MODULES}
_ORIGINAL_BUILDER = SparkSession.__dict__["builder"]

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/runtime"
os.environ["FABRICKS_CONFIG"] = "tests/spark/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_DEBUGMODE"] = "FALSE"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

_fake_spark_session = MagicMock(spec=SparkSession, name="fake_spark_session")
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


def _make_builder() -> MagicMock:
    builder = MagicMock(spec=SparkSession.Builder, name="fake_spark_session_builder")
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.enableHiveSupport.return_value = builder
    builder.getOrCreate.return_value = _fake_spark_session
    return builder


SparkSession.builder = _make_builder()  # fabricks.context builds SPARK at import, before any fixture runs


def _clear_configured_results(mock: NonCallableMock) -> None:
    """Drop what a test configured (return_value, side_effect) on `mock` and its attribute children.

    Not `reset_mock(return_value=True, side_effect=True)`: that also resets the magic-method defaults
    (`__bool__` -> True), after which `if not SPARK` raises TypeError. Dunder children are left alone."""
    mock.side_effect = None
    mock.return_value = DEFAULT
    for name, child in list(mock._mock_children.items()):
        if not name.startswith("__") and isinstance(child, NonCallableMock):
            _clear_configured_results(child)


def _reset_bootstrap_mocks() -> None:
    """The shared bootstrap mocks cannot be replaced per test (21 modules hold `SPARK` by value)."""
    for mock in (_fake_spark_session, _fake_dbutils):
        mock.reset_mock()  # call records
        _clear_configured_results(mock)


@pytest.fixture(autouse=True)
def _reset_bootstrap_mocks_fixture(monkeypatch):
    _reset_bootstrap_mocks()
    monkeypatch.setattr(SparkSession, "builder", _make_builder())
    yield
    _reset_bootstrap_mocks()


@pytest.fixture(scope="session", autouse=True)
def _restore_process_state():
    """Only matters when pytest runs more than once in a process (REPL, IDE runner, nested run): the
    runtests.py runners call pytest.main once, so this is otherwise a no-op. Kept so this conftest
    leaves no process-wide state behind."""
    yield
    SparkSession.builder = _ORIGINAL_BUILDER
    for name, original in _ORIGINAL_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original
    for key, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


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
