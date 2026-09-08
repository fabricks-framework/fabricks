"""Generator.drop() (framework/fabricks/core/jobs/base/generator.py:122-160):
the `options.no_drop` guard (137-138) raises before anything else runs -
`self.spark.sql(...)` right after it is wrapped in a bare
`except Exception: pass`, so the guard is the only thing this test needs to
isolate.
"""

import pytest

from fabricks.core import get_job


def _job(*, no_drop: bool | None = None):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(update={"options": job.conf.options.model_copy(update={"no_drop": no_drop})})
    return job


def test_drop_raises_when_no_drop_is_set():
    job = _job(no_drop=True)

    with pytest.raises(ValueError, match="no_drop"):
        job.drop()


def test_drop_does_not_raise_when_no_drop_unset():
    job = _job()

    job.drop()  # must not raise
