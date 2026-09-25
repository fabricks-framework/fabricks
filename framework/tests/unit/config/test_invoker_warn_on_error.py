"""JobInvoker.invoke_job/_invoke_step (framework/fabricks/core/jobs/base/
invoker.py): a per-invoker `warn_on_error=True` option turns a failing
notebook invoker into a logged warning instead of a typed
Pre/PostRunInvokeException, so the job run neither fails nor (as a
consequence, per job.py's run()) triggers self.restore().

Default behavior (warn_on_error unset/False) is unchanged: the invoker
failure still raises the typed exception, per
test_invoker_exception_typing.py.
"""

import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PostRunInvokeException, PreRunInvokeException
from fabricks.models.common import BaseInvokerOptions, InvokerOptions


def _job_with_invoker(position: str, warn_on_error: bool | None):
    job = get_job(step="gold", topic="fact", item="step_option")
    invoker = BaseInvokerOptions(notebook="dummy", warn_on_error=warn_on_error)
    job.conf = job.conf.model_copy(update={"invoker_options": InvokerOptions(**{position: [invoker]})})
    return job


@pytest.mark.parametrize(
    ("position", "exception"), [("post_run", PostRunInvokeException), ("pre_run", PreRunInvokeException)]
)
def test_warn_on_error_true_does_not_raise(monkeypatch, position, exception):
    job = _job_with_invoker(position, warn_on_error=True)
    monkeypatch.setattr(
        job._invoker, "_invoke_notebook", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    job._invoker.invoke_job(position=position)  # must not raise


@pytest.mark.parametrize(
    ("warn_on_error", "position", "exception"),
    [
        (None, "post_run", PostRunInvokeException),
        (False, "post_run", PostRunInvokeException),
        (None, "pre_run", PreRunInvokeException),
        (False, "pre_run", PreRunInvokeException),
    ],
)
def test_warn_on_error_default_and_false_still_raise(monkeypatch, warn_on_error, position, exception):
    job = _job_with_invoker(position, warn_on_error=warn_on_error)
    monkeypatch.setattr(
        job._invoker, "_invoke_notebook", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(exception):
        job._invoker.invoke_job(position=position)
