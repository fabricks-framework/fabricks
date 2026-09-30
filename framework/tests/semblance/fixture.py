"""pytest plugin exposing `semblance`: fresh, strict fakes for the Azure and Databricks boundary.

Loaded via `pytest_plugins` in tests/conftest.py at collection time -- before the tier conftests
bootstrap the fake fabricks environment -- so nothing here may import fabricks at module level.
"""

import importlib
from pathlib import Path
import sys
import time
import types
from typing import Any

import pytest

from tests.semblance.azure_fakes import (
    QueueClientFactory,
    QueueStore,
    QueueView,
    TableServiceFactory,
    TableStore,
    TableView,
)
from tests.semblance.dbutils_fake import DbutilsState, FakeDbutils

_DEV_ACCOUNT = "semblance"


class Semblance:
    """What a test sees. Seed through the dicts and views, assert through the views."""

    def __init__(self, fs_root: Path) -> None:
        self._state = DbutilsState(fs_root=fs_root)
        self.dbutils = FakeDbutils(self._state)
        self.tables = TableStore()
        self.queues = QueueStore()

    @property
    def widgets(self) -> dict[str, str]:
        return self._state.widgets

    @property
    def secrets(self) -> dict[tuple[str, str], str]:
        return self._state.secrets

    @property
    def task_values(self) -> dict[str, Any]:
        return self._state.task_values

    @property
    def notebook_calls(self):
        return self._state.notebook_calls

    @property
    def fs_root(self) -> Path:
        return self._state.fs_root

    def table(self, name: str) -> TableView:
        return TableView(self.tables, name)

    def queue(self, name: str) -> QueueView:
        return QueueView(self.queues, name)

    def on_notebook_run(self, returns: Any, path: str | None = None) -> None:
        """Script dbutils.notebook.run: a status string, or a callable(NotebookCall) -> status. An
        unregistered run raises AssertionError."""
        self._state.notebook_results.append((path, returns))


@pytest.fixture
def no_real_sleep(monkeypatch):
    """Skip real waits (tenacity backoff, DagGenerator's time.sleep(60)); yield to other threads."""
    real_sleep = time.sleep
    monkeypatch.setattr(time, "sleep", lambda *_a, **_kw: real_sleep(0))


@pytest.fixture
def semblance(monkeypatch, tmp_path, no_real_sleep):
    s = Semblance(tmp_path)

    # Azure SDK boundary
    monkeypatch.setattr("fabricks.utils.azure_table.TableServiceClient", TableServiceFactory(s.tables))
    monkeypatch.setattr("fabricks.utils.azure_queue.QueueClient", QueueClientFactory(s.queues))

    # databricks.sdk.runtime: always a fresh module. Only the config tier pre-fakes it; the plain
    # tier does not, and importing the real one tries to authenticate against a workspace.
    fabricks_spark = sys.modules.get("fabricks.utils.spark") or importlib.import_module("fabricks.utils.spark")
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = s.dbutils  # type: ignore[attr-defined]
    runtime.spark = fabricks_spark.spark  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", runtime)
    monkeypatch.setattr(fabricks_spark, "dbutils", s.dbutils, raising=False)

    # Only when the test already imported the DAG modules (import them at module top, as tests do).
    base = sys.modules.get("fabricks.core.dags.base")
    if base is not None:
        connection = {"storage_account": _DEV_ACCOUNT, "access_key": "semblance-key", "credential": None}
        monkeypatch.setattr(base.BaseDags, "get_connection_info", lambda self: connection)

    log = sys.modules.get("fabricks.core.dags.log")
    if log is not None:
        from fabricks.utils.azure_table import AzureTable

        table = AzureTable("dags", storage_account=_DEV_ACCOUNT, access_key="semblance-key")
        monkeypatch.setattr(log.TABLE_LOG_HANDLER, "_table", table)

    return s
