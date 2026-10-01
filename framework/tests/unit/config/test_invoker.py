"""JobInvoker behavior: notebook retry, error typing and `warn_on_error`, and the retry seen from `job.run()`.

Retry, https://github.com/fabricks-framework/fabricks/issues/189: a transient `dbutils.notebook.run()`
failure (e.g. a Py4JJavaError from a JDBC blip in a pre_run notebook) used to propagate immediately and the
job was reported failed even if a later retry succeeded outside Fabricks. An invoker can now opt in to one
retry with `retry: true` (default off). Without `retry_on_error` any exception retries; with it (e.g.
["Py4JJavaError", "TimeoutError"]) only the named types do.
"""

import sys
from unittest.mock import MagicMock

from py4j.protocol import Py4JJavaError
import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.models.common import BaseInvokerOptions, InvokerOptions
from fabricks.utils.path import GitPath
from tests.unit.config._helpers import stub_table

pytestmark = pytest.mark.usefixtures("no_real_sleep")

dbr = sys.modules["databricks.sdk.runtime"]  # faked by tests/unit/config/conftest.py

_EXCEPTIONS = {"pre_run": PreRunInvokeException, "post_run": PostRunInvokeException}


def _py4j_error() -> Py4JJavaError:
    return Py4JJavaError("connection reset by peer", MagicMock())


def _invoker():
    return get_job(step="gold", topic="fact", item="step_option")._invoker


def _run_notebook(*, retry: bool | None = None, retry_on_error: list[str] | None = None):
    kwargs: dict[str, object] = {} if retry is None else {"retry": retry}
    if retry_on_error is not None:
        kwargs["retry_on_error"] = retry_on_error
    return _invoker()._run_notebook(path=GitPath("dummy_notebook"), arguments={}, timeout=60, schedule=None, **kwargs)


def _job_with_invoker(position: str, **options):
    job = get_job(step="gold", topic="fact", item="step_option")
    invoker = BaseInvokerOptions(notebook="dummy", **options)
    job.conf = job.conf.model_copy(update={"invoker_options": InvokerOptions(**{position: [invoker]})})
    return job


def _failing_invoker(monkeypatch, job) -> list[int]:
    """Make every notebook invocation fail; the returned list gets one entry per attempted invocation."""
    attempts: list[int] = []

    def _boom(*_a, **_kw):
        attempts.append(1)
        raise RuntimeError("boom")

    monkeypatch.setattr(job._invoker, "_invoke_notebook", _boom)
    return attempts


# --- _run_notebook retry -----------------------------------------------------------------------------------------


def test_retry_off_by_default_raises_immediately(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        raise _py4j_error()

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    with pytest.raises(Py4JJavaError):
        _run_notebook()
    assert calls["n"] == 1


def test_retry_true_no_retry_on_error_retries_any_exception(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("even a non-transient-looking error retries with no retry_on_error")
        return "ok"

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    assert _run_notebook(retry=True) == "ok"
    assert calls["n"] == 2


def test_retry_true_persistent_failure_still_raises_after_exactly_one_retry(monkeypatch):
    calls = {"n": 0}

    def always_flaky(*_a, **_kw):
        calls["n"] += 1
        raise _py4j_error()

    monkeypatch.setattr(dbr.dbutils.notebook, "run", always_flaky)

    with pytest.raises(Py4JJavaError):
        _run_notebook(retry=True)
    assert calls["n"] == 2


def test_retry_on_error_retries_named_type(monkeypatch):
    calls = {"n": 0}

    def flaky_run(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 2:
            raise _py4j_error()
        return "ok"

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_run)

    assert _run_notebook(retry=True, retry_on_error=["Py4JJavaError"]) == "ok"
    assert calls["n"] == 2


def test_retry_on_error_does_not_retry_unlisted_type(monkeypatch):
    calls = {"n": 0}

    def bad_config(*_a, **_kw):
        calls["n"] += 1
        raise ValueError("not in the retry_on_error list")

    monkeypatch.setattr(dbr.dbutils.notebook, "run", bad_config)

    with pytest.raises(ValueError, match="not in the retry_on_error list"):
        _run_notebook(retry=True, retry_on_error=["Py4JJavaError"])
    assert calls["n"] == 1


def test_retry_on_error_unknown_name_raises(monkeypatch):
    monkeypatch.setattr(dbr.dbutils.notebook, "run", lambda *_a, **_kw: "ok")

    with pytest.raises(AssertionError, match="unknown exception name"):
        _run_notebook(retry=True, retry_on_error=["NotARealException"])


# --- error typing and warn_on_error ------------------------------------------------------------------------------


@pytest.mark.parametrize("position", ["post_run", "pre_run"])
def test_failed_invoker_raises_typed_exception(monkeypatch, position):
    job = _job_with_invoker(position)
    attempts = _failing_invoker(monkeypatch, job)

    with pytest.raises(_EXCEPTIONS[position], match="boom"):
        job._invoker.invoke_job(position=position)
    assert attempts == [1]


@pytest.mark.parametrize("position", ["post_run", "pre_run"])
def test_warn_on_error_true_does_not_raise(monkeypatch, position):
    job = _job_with_invoker(position, warn_on_error=True)
    attempts = _failing_invoker(monkeypatch, job)

    job._invoker.invoke_job(position=position)

    assert attempts == [1], "the failing invoker must actually have run for 'does not raise' to mean anything"


@pytest.mark.parametrize("warn_on_error", [None, False])
@pytest.mark.parametrize("position", ["post_run", "pre_run"])
def test_warn_on_error_default_and_false_still_raise(monkeypatch, warn_on_error, position):
    job = _job_with_invoker(position, warn_on_error=warn_on_error)
    _failing_invoker(monkeypatch, job)

    with pytest.raises(_EXCEPTIONS[position], match="boom"):
        job._invoker.invoke_job(position=position)


# --- retry as seen from job.run() ---------------------------------------------------------------------------------


def _job_with_flaky_pre_run(monkeypatch, retry: bool, calls: dict):
    job = _job_with_invoker("pre_run", retry=retry)

    def flaky_notebook_run(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 2:
            # A plain builtin, not Py4JJavaError: Py4JJavaError.__str__() needs a
            # real java gateway and would crash str(e) in _raise_invoke_errors --
            # a test-mock artifact, unrelated to the fix under test.
            raise ConnectionError("connection reset by peer")
        return "ok"

    monkeypatch.setattr(dbr.dbutils.notebook, "run", flaky_notebook_run)

    # Everything after pre_run is irrelevant to this claim -- stub it out so
    # only the pre_run invoker step (and job.run()'s own control flow around
    # it) is actually exercised.
    monkeypatch.setattr(job._checker, "run_before", lambda: None)
    monkeypatch.setattr(job._checker, "run_after", lambda: None)
    monkeypatch.setattr(job._checker, "skip_run", lambda: None)
    monkeypatch.setattr(job._checker, "pre_run", lambda: None)
    monkeypatch.setattr(job, "for_each_run", lambda *_a, **_kw: None)
    monkeypatch.setattr(job._checker, "post_run", lambda: None)
    monkeypatch.setattr(job._checker, "post_run_extra", lambda: None)
    monkeypatch.setattr(job._invoker, "post_run", lambda schedule=None: None)
    stub_table(monkeypatch, job)

    return job


def test_job_run_completes_when_pre_run_invoker_recovers_on_retry(monkeypatch):
    calls = {"n": 0}
    job = _job_with_flaky_pre_run(monkeypatch, retry=True, calls=calls)

    job.run(schedule=None, schedule_id="x")

    assert calls["n"] == 2


def test_job_run_fails_when_pre_run_invoker_has_no_retry(monkeypatch):
    calls = {"n": 0}
    job = _job_with_flaky_pre_run(monkeypatch, retry=False, calls=calls)

    with pytest.raises(PreRunInvokeException, match="connection reset by peer"):
        job.run(schedule=None, schedule_id="x")
    assert calls["n"] == 1
