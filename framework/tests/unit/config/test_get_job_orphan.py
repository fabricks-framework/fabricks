"""get_job(orphan=True) (framework/fabricks/core/jobs/get_job.py) returns
an OrphanJob built purely from step/topic/item, with no config lookup --
see https://github.com/fabricks-framework/fabricks/issues/198.
"""

import pytest

from fabricks.core.jobs import OrphanJob, get_job


def test_get_job_orphan_returns_an_orphan_job():
    job = get_job(step="silver", topic="foo", item="bar", orphan=True)

    assert isinstance(job, OrphanJob)
    assert (job.step, job.topic, job.item) == ("silver", "foo", "bar")


def test_get_job_orphan_defaults_to_false():
    from fabricks.core.jobs.silver import Silver

    job = get_job(step="silver", topic="append_test", item="test")

    assert isinstance(job, Silver)


def test_get_job_orphan_rejects_job_id():
    with pytest.raises(AssertionError):
        get_job(step="silver", topic="foo", item="bar", job_id="deadbeef", orphan=True)  # ty: ignore[no-matching-overload]
