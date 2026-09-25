"""BaseJob.run() (framework/fabricks/core/jobs/base/job.py): a pre_run
invoker configured with `retry: true` that fails once with a transient
error and succeeds on the internal retry must let job.run() complete
normally -- issue #189's actual claim is that this keeps the job's
lifecycle logging intact (one run() call, "done" logged once), so
DagTerminator never sees it as failed.

Everything in job.run() unrelated to the pre_run invoker step (checks,
the actual data write, post_run, maintenance) is stubbed out here -- this
test isolates the one claim: job.run() must not raise when pre_run's
notebook invoker recovers on retry.
"""

import sys

import pytest

from fabricks.core import get_job
from fabricks.models.common import BaseInvokerOptions, InvokerOptions

pytestmark = pytest.mark.usefixtures("no_real_sleep")

dbr = sys.modules["databricks.sdk.runtime"]  # faked by tests/unit/config/conftest.py


def _job_with_flaky_pre_run(monkeypatch, retry: bool, calls: dict):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={"invoker_options": InvokerOptions(pre_run=[BaseInvokerOptions(notebook="dummy", retry=retry)])}
    )

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

    return job


def test_job_run_completes_when_pre_run_invoker_recovers_on_retry(monkeypatch):
    calls = {"n": 0}
    job = _job_with_flaky_pre_run(monkeypatch, retry=True, calls=calls)

    job.run(schedule=None, schedule_id="x")  # must not raise

    assert calls["n"] == 2


def test_job_run_fails_when_pre_run_invoker_has_no_retry(monkeypatch):
    calls = {"n": 0}
    job = _job_with_flaky_pre_run(monkeypatch, retry=False, calls=calls)

    with pytest.raises(Exception, match="connection reset by peer"):
        job.run(schedule=None, schedule_id="x")
    assert calls["n"] == 1
