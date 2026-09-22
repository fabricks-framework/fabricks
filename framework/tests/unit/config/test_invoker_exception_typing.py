"""JobInvoker.invoke_job/_invoke_step (framework/fabricks/core/jobs/base/
invoker.py): a notebook invoker failure must surface as the typed
Pre/PostRunInvokeException, not a bare Exception.

Regression test for https://github.com/fabricks-framework/fabricks/issues/188:
job.py's run() has a dedicated `except (PreRunInvokeException,
PostRunInvokeException)` handler specifically so invoker failures don't
trigger self.restore() -- the data write already succeeded, only the
notebook invoker failed. But invoke_job()/_invoke_step() collect the typed
exceptions into a list and then `raise Exception("; ".join(...))`, which
is not an instance of PostRunInvokeException, so it falls through to
run()'s generic `except Exception` handler instead and restores the table,
silently discarding a successful write.
"""

import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.models.common import BaseInvokerOptions, InvokerOptions


def _job_with_post_run_invoker():
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(
        update={"invoker_options": InvokerOptions(post_run=[BaseInvokerOptions(notebook="dummy")])}
    )
    return job


def test_failed_post_run_invoker_raises_typed_exception(monkeypatch):
    job = _job_with_post_run_invoker()
    monkeypatch.setattr(
        job._invoker, "_invoke_notebook", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(PostRunInvokeException):
        job._invoker.invoke_job(position="post_run")


def test_failed_pre_run_invoker_raises_typed_exception(monkeypatch):
    job = _job_with_post_run_invoker()
    job.conf = job.conf.model_copy(
        update={"invoker_options": InvokerOptions(pre_run=[BaseInvokerOptions(notebook="dummy")])}
    )
    monkeypatch.setattr(
        job._invoker, "_invoke_notebook", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(PreRunInvokeException):
        job._invoker.invoke_job(position="pre_run")
