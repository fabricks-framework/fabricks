"""Live notebook-derived Gold dependency persistence.

gold.dim_time is already tagged and run by the schedule (conftest.py's
_schedule_run) -- update_dependencies() just needs it and
gold.dependency_notebook (also tagged) to already exist, not a fresh run.
"""

from fabricks.context import SPARK
from fabricks.core import get_job
from fabricks.core.steps import get_step
from fabricks.models import get_dependency_id, get_job_id


def test_notebook_derived_gold_dependency_is_persisted():
    job = get_job(step="gold", topic="dependency", item="notebook")

    get_step("gold").update_dependencies()
    get_step("gold").update_dependencies()

    rows = SPARK.sql(
        f"""select dependency_id, job_id, parent_id, parent, origin
        from fabricks.gold_dependencies
        where job_id = '{job.job_id}'"""
    ).collect()
    assert [(row.dependency_id, row.job_id, row.parent_id, row.parent, row.origin) for row in rows] == [
        (
            get_dependency_id("gold.dim_time", job.job_id),
            job.job_id,
            get_job_id(job="gold.dim_time"),
            "gold.dim_time",
            "parser",
        )
    ]
