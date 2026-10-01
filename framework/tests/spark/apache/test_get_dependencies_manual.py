"""options.type == "manual" marks a job as excluded from automatic dependency resolution (it is run out of band);
`BaseStep._get_dependencies_internal` enforces it with a SQL predicate, so it is proven against a real DataFrame."""

import pytest

from fabricks.core.steps import get_step
import fabricks.core.steps.base as steps_base


@pytest.fixture
def dispatched_job_ids(local_spark, monkeypatch):
    jobs = local_spark.sql(
        """
        select 'manual_job' as job_id, named_struct('type', 'manual') as options
        union all select 'default_job', named_struct('type', 'default')
        union all select 'untyped_job', named_struct('type', cast(null as string))
        """
    )
    step = get_step("gold")
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: jobs)

    seen: list[list[str]] = []

    def _capture(_fn, df, **_kwargs):
        seen.append(sorted(row.job_id for row in df.collect()))
        return []

    monkeypatch.setattr(steps_base, "run_in_parallel", _capture)
    return step, seen


def test_get_dependencies_internal_excludes_manual_jobs_by_default(dispatched_job_ids):
    step, seen = dispatched_job_ids

    step._get_dependencies_internal()

    assert seen == [["default_job", "untyped_job"]], "a null type is not manual and must stay in"


def test_get_dependencies_internal_keeps_manual_jobs_when_requested(dispatched_job_ids):
    step, seen = dispatched_job_ids

    step._get_dependencies_internal(include_manual=True)

    assert seen == [["default_job", "manual_job", "untyped_job"]]
