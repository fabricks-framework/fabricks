"""JobInvoker._run_notebook (framework/fabricks/core/jobs/base/invoker.py):
a transient dbutils.notebook.run() failure (e.g. a JVM-level Py4JJavaError
from a JDBC connection blip in a pre_run invoker notebook) currently
propagates immediately, with no retry anywhere between it and job.py's
run() -- see issue #189. Even though the item is often re-dispatched and
succeeds outside Fabricks (a Databricks Workflow task retry), the first
attempt's failure already logged, and the DagTerminator later reports the
job as failed regardless of that later success.

This test encodes the fixed (Option A) behavior: an invoker can opt in to a
single retry via `retry: true` (default off, so existing invokers are
unaffected). With no `retry_on_error`, retry fires on any exception; with
it (a list of known names, e.g. ["Py4JJavaError", "TimeoutError"]), retry
fires only on those named type(s).
"""

import sys
import time
from unittest.mock import MagicMock

from py4j.protocol import Py4JJavaError
import pytest

from fabricks.core import get_job
from fabricks.utils.path import GitPath

dbr = sys.modules["databricks.sdk.runtime"]  # faked by tests/unit/config/conftest.py


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    # invoker.py's retry uses wait_fixed(60) for real; skip the actual wait here.
    monkeypatch.setattr(time, "sleep", lambda *_a, **_kw: None)


def _py4j_error() -> Py4JJavaError:
    return Py4JJavaError("connection reset by peer", MagicMock())


def _invoker():
    job = get_job(step="gold", topic="fact", item="step_option")
    return job._invoker


def test_retry_off_by_default_raises_immediately(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        raise _py4j_error()

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    with pytest.raises(Py4JJavaError):
        _invoker()._run_notebook(path=GitPath("dummy_notebook"), arguments={}, timeout=60, schedule=None)
    assert calls["n"] == 1


def test_retry_true_no_retry_on_error_retries_any_exception(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("even a non-transient-looking error retries with no retry_on_error")
        return "ok"

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    result = _invoker()._run_notebook(
        path=GitPath("dummy_notebook"), arguments={}, timeout=60, schedule=None, retry=True
    )

    assert result == "ok"
    assert calls["n"] == 2


def test_retry_true_persistent_failure_still_raises(monkeypatch):
    def always_flaky(*_a, **_kw):
        raise _py4j_error()

    monkeypatch.setattr(dbr.dbutils.notebook, "run", always_flaky)

    with pytest.raises(Py4JJavaError):
        _invoker()._run_notebook(path=GitPath("dummy_notebook"), arguments={}, timeout=60, schedule=None, retry=True)


def test_retry_on_error_retries_named_type(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise _py4j_error()
        return "ok"

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    result = _invoker()._run_notebook(
        path=GitPath("dummy_notebook"),
        arguments={},
        timeout=60,
        schedule=None,
        retry=True,
        retry_on_error=["Py4JJavaError"],
    )

    assert result == "ok"
    assert calls["n"] == 2


def test_retry_on_error_does_not_retry_unlisted_type(monkeypatch):
    calls = {"n": 0}

    def bad_config(*_a, **_kw):
        calls["n"] += 1
        raise ValueError("not in the retry_on_error list")

    monkeypatch.setattr(dbr.dbutils.notebook, "run", bad_config)

    with pytest.raises(ValueError, match="not in the retry_on_error list"):
        _invoker()._run_notebook(
            path=GitPath("dummy_notebook"),
            arguments={},
            timeout=60,
            schedule=None,
            retry=True,
            retry_on_error=["Py4JJavaError"],
        )
    assert calls["n"] == 1


def test_retry_on_error_unknown_name_raises(monkeypatch):
    monkeypatch.setattr(dbr.dbutils.notebook, "run", lambda *_a, **_kw: "ok")

    with pytest.raises(AssertionError, match="unknown exception name"):
        _invoker()._run_notebook(
            path=GitPath("dummy_notebook"),
            arguments={},
            timeout=60,
            schedule=None,
            retry=True,
            retry_on_error=["NotARealException"],
        )
