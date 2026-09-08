"""Configurator.timeout / _get_timeout (framework/fabricks/core/jobs/base/
configurator.py:140-153): job-level options.timeout -> step-level
step_options.timeouts.job -> runtime_options.timeouts.job, in that order.
The runtime fallback (3600) comes from tests/spark/apache/runtime/fabricks/
conf.fabricks.yml's `options.timeouts.job`; the "gold" step there
declares no `timeouts` of its own, so it's also the step-level test's
starting point.
"""

from fabricks.core import get_job
from fabricks.models import StepTimeoutOptions


def _job():
    return get_job(step="gold", topic="fact", item="step_option")


def test_timeout_job_level_wins():
    job = _job()
    job.conf = job.conf.model_copy(update={"options": job.conf.options.model_copy(update={"timeout": 42})})

    assert job.timeout == 42


def test_timeout_falls_back_to_step_level_when_job_level_unset():
    job = _job()
    step_conf = job.step_conf
    new_options = step_conf.options.model_copy(update={"timeouts": StepTimeoutOptions(job=1800)})
    job.base_step_conf = step_conf.model_copy(update={"options": new_options})

    assert job.timeout == 1800


def test_timeout_falls_back_to_runtime_when_neither_job_nor_step_set():
    job = _job()

    assert job.timeout == 3600
