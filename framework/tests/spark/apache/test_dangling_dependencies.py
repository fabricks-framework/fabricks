"""Issue #264: an edge whose parent is not a Fabricks job is dropped by the scheduler, so the job runs without waiting
for it. `check_dependencies` lists (and warns about) exactly those edges, for a step or a single job."""

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from fabricks.core.steps import get_step
from fabricks.models import get_dependency_id, get_job_id

_JOB = "silver.king_and_queen_scd1"
_MISSING = "bronze.does_not_exist"


def _arrange(spark):
    # the parents live in bronze, so its jobs table must be registered for healthy edges to resolve
    get_step("bronze").update_configurations()
    step = get_step("silver")
    step.update_configurations()
    step.update_dependencies()

    job_id = get_job_id(job=_JOB)
    spark.sql(
        f"insert into fabricks.silver_dependencies (dependency_id, job_id, parent_id, parent, origin) "
        f"values ('{get_dependency_id(_MISSING, job_id)}', '{job_id}', '{get_job_id(job=_MISSING)}', '{_MISSING}', "
        f"'parent')"
    )
    return step


def test_check_dependencies_lists_only_edges_whose_parent_is_not_a_job(local_spark):
    step = _arrange(local_spark)

    rows = step.check_dependencies()

    assert [(r.parent, r.origin) for r in rows if r.job == _JOB] == [(_MISSING, "parent")], (
        "only the edge to the missing parent is dangling; bronze.king_scd1 and bronze.queen_scd1 are real jobs"
    )


def test_job_update_dependencies_warns_about_its_dangling_parent(local_spark, monkeypatch):
    _arrange(local_spark)
    warnings = []
    monkeypatch.setattr(DEFAULT_LOGGER, "warning", lambda msg, *a, **kw: warnings.append(msg))

    job = get_job(job=_JOB)
    job.conf = job.conf.model_copy(
        update={"options": job.conf.options.model_copy(update={"parents": ["bronze.king_scd1", _MISSING]})}
    )
    job.update_dependencies()

    assert [w for w in warnings if _MISSING in w] == [
        f"{_MISSING} (parent) is not a fabricks job, the scheduler ignores this dependency"
    ]
