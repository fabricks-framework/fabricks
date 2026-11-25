"""Issue #264: a parent written with uppercase letters (e.g. a SQL reference `raw.onetrade_STKLAR`) must resolve to
the same parent_id as the lowercase job, or the scheduler drops the edge and the job runs without waiting."""

from fabricks.models import JobDependency, get_dependency_id, get_job_id


def test_parent_is_matched_case_insensitively():
    dep = JobDependency.from_parts("child", "Raw.Onetrade_STKLAR__current", "parser")

    assert (dep.parent, dep.parent_id, dep.dependency_id) == (
        "raw.onetrade_stklar",
        get_job_id(job="raw.onetrade_stklar"),
        get_dependency_id(parent="raw.onetrade_stklar", job_id="child"),
    )
