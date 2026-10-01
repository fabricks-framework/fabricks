"""JobInvoker behavior: notebook retry, error typing and `warn_on_error`, and the retry seen from `job.run()`.

Retry, https://github.com/fabricks-framework/fabricks/issues/189: a transient `dbutils.notebook.run()`
failure (e.g. a Py4JJavaError from a JDBC blip in a pre_run notebook) used to propagate immediately and the
job was reported failed even if a later retry succeeded outside Fabricks. An invoker can now opt in to one
retry with `retry: true` (default off). Without `retry_on_error` any exception retries; with it (e.g.
["Py4JJavaError", "TimeoutError"]) only the named types do.
"""

from unittest.mock import MagicMock

from py4j.protocol import Py4JJavaError
import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.models.common import BaseInvokerOptions, InvokerOptions
from fabricks.utils.path import GitPath
from tests.unit.config._helpers import stub_table

pytestmark = pytest.mark.usefixtures("no_real_sleep")

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


def _script_notebook(semblance, *outcomes: object) -> None:
    """The Nth `dbutils.notebook.run` returns/raises outcomes[N]; the last one repeats. Calls land in semblance."""

    def _outcome(_call) -> object:
        outcome = outcomes[min(len(semblance.notebook_calls), len(outcomes)) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    semblance.on_notebook_run(returns=_outcome)


def test_retry_off_by_default_raises_immediately(semblance):
    _script_notebook(semblance, _py4j_error())

    with pytest.raises(Py4JJavaError):
        _run_notebook()
    assert len(semblance.notebook_calls) == 1


def test_retry_true_no_retry_on_error_retries_any_exception(semblance):
    # even a non-transient-looking error retries with no retry_on_error
    _script_notebook(semblance, ValueError("flaky"), "ok")

    assert _run_notebook(retry=True) == "ok"
    assert len(semblance.notebook_calls) == 2


def test_retry_true_persistent_failure_still_raises_after_exactly_one_retry(semblance):
    _script_notebook(semblance, _py4j_error())

    with pytest.raises(Py4JJavaError):
        _run_notebook(retry=True)
    assert len(semblance.notebook_calls) == 2


def test_retry_on_error_retries_named_type(semblance):
    _script_notebook(semblance, _py4j_error(), "ok")

    assert _run_notebook(retry=True, retry_on_error=["Py4JJavaError"]) == "ok"
    assert len(semblance.notebook_calls) == 2


def test_retry_on_error_does_not_retry_unlisted_type(semblance):
    _script_notebook(semblance, ValueError("not in the retry_on_error list"))

    with pytest.raises(ValueError, match="not in the retry_on_error list"):
        _run_notebook(retry=True, retry_on_error=["Py4JJavaError"])
    assert len(semblance.notebook_calls) == 1


def test_retry_on_error_unknown_name_raises(semblance):
    _script_notebook(semblance, "ok")

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


def _job_with_flaky_pre_run(monkeypatch, semblance, retry: bool):
    job = _job_with_invoker("pre_run", retry=retry)
    # A plain builtin, not Py4JJavaError: Py4JJavaError.__str__() needs a real java gateway.
    _script_notebook(semblance, ConnectionError("connection reset by peer"), "ok")

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


def test_job_run_completes_when_pre_run_invoker_recovers_on_retry(monkeypatch, semblance):
    job = _job_with_flaky_pre_run(monkeypatch, semblance, retry=True)

    job.run(schedule=None, schedule_id="x")

    assert len(semblance.notebook_calls) == 2


def test_job_run_fails_when_pre_run_invoker_has_no_retry(monkeypatch, semblance):
    job = _job_with_flaky_pre_run(monkeypatch, semblance, retry=False)

    with pytest.raises(PreRunInvokeException, match="connection reset by peer"):
        job.run(schedule=None, schedule_id="x")
    assert len(semblance.notebook_calls) == 1
