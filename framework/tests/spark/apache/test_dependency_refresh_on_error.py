"""Issue #262: a job whose dependency resolution fails must keep its existing edges. The refresh merge treated every
in-scope job missing from the source as having no parents, so one failure wiped that job's dependencies and the
scheduler ran it without waiting for its parents."""

import pytest

from fabricks.core.jobs.gold import Gold
from fabricks.core.jobs.silver import Silver
from fabricks.core.steps import get_step
from fabricks.models import get_dependency_id, get_job_id

# (step, job, its topic, the edges a healthy refresh writes, where the fault is injected)
_CASES = [
    pytest.param(
        "silver",
        "silver.append_test_test",
        "append_test",
        {("bronze.append_test_test", "parser")},
        (Silver, "get_dependencies"),
        id="silver",
    ),
    pytest.param(
        "gold",
        "gold.depchain_view_d",
        "depchain",
        {("gold.depchain_view_b", "parser"), ("gold.depchain_view_c", "parser")},
        (Gold, "_get_sql_dependencies"),
        id="gold",
    ),
]


def _edges(spark, step: str, job_id: str) -> set[tuple[str, str]]:
    rows = spark.sql(f"select parent, origin from fabricks.{step}_dependencies where job_id = '{job_id}'").collect()
    return {(r.parent, r.origin) for r in rows}


@pytest.mark.parametrize(("step_name", "job", "topic", "expected", "fault"), _CASES)
@pytest.mark.parametrize("refresh_topic", [False, True], ids=["full-refresh", "topic-refresh"])
def test_failed_dependency_resolution_keeps_existing_edges(
    local_spark, monkeypatch, step_name, job, topic, expected, fault, refresh_topic
):
    step = get_step(step_name)
    step.update_configurations()
    # topic-scoped: the gold test runtime has jobs without a .sql file, so a full gold refresh always raises
    step.update_dependencies(topic=topic)

    job_id = get_job_id(job=job)
    assert _edges(local_spark, step_name, job_id) == expected, "precondition: the job has its edges"

    cls, method = fault
    original = getattr(cls, method)

    # fault injection: stands in for any transient failure (rendering, reading the SQL, Spark) on one job
    def flaky(self, *args, **kwargs):
        if self.job_id == job_id:
            raise RuntimeError("transient failure resolving dependencies")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(cls, method, flaky)

    with pytest.raises(Exception, match=r"transient failure|update dependencies"):
        step.update_dependencies(topic=topic if refresh_topic else None)

    assert _edges(local_spark, step_name, job_id) == expected, "a failed resolution must not delete existing edges"


@pytest.mark.parametrize("refresh_topic", [False, True], ids=["full-refresh", "topic-refresh"])
def test_failed_job_does_not_shield_siblings_from_refresh(local_spark, monkeypatch, refresh_topic):
    """Control for #262: only the failing job leaves the merge scope; a sibling's stale edge is still deleted."""
    step = get_step("gold")
    step.update_configurations()
    step.update_dependencies(topic="depchain")

    failing_id = get_job_id(job="gold.depchain_view_d")
    sibling_id = get_job_id(job="gold.depchain_view_b")
    stale = "gold.no_longer_read"
    local_spark.sql(
        f"insert into fabricks.gold_dependencies (dependency_id, job_id, parent, origin) "
        f"values ('{get_dependency_id(stale, sibling_id)}', '{sibling_id}', '{stale}', 'parent')"
    )

    original = Gold._get_sql_dependencies

    def flaky(self):
        if self.job_id == failing_id:
            raise RuntimeError("transient failure resolving dependencies")
        return original(self)

    monkeypatch.setattr(Gold, "_get_sql_dependencies", flaky)

    with pytest.raises(Exception, match=r"transient failure|update dependencies"):
        step.update_dependencies(topic="depchain" if refresh_topic else None)

    assert _edges(local_spark, "gold", sibling_id) == {("gold.depchain_view_c", "parser")}, (
        "the sibling's stale edge must be deleted and its parser edge kept"
    )
