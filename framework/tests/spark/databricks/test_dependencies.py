"""Live Gold/Silver dependency persistence, read back from
fabricks.<step>_dependencies.

The gold test reads what the schedule already persisted (gold.dim_time and
gold.dependency_notebook are tagged and run by conftest.py's _schedule_run).
The silver test re-runs update_dependencies() twice to prove it's
idempotent, and pins every edge, including both origins: explicit `parents`
("parent") and the default same-name bronze parent ("parser", used by
silver.queen_scd1, which sets no `parents`).
"""

from fabricks.context import SPARK
from fabricks.core import get_job
from fabricks.core.steps import get_step
from fabricks.models import get_dependency_id, get_job_id


def test_notebook_derived_gold_dependency_is_persisted():
    job = get_job(step="gold", topic="dependency", item="notebook")

    rows = SPARK.sql(
        f"""
        select dependency_id, job_id, parent_id, parent, origin
        from fabricks.gold_dependencies
        where job_id = '{job.job_id}'
        """
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


def test_silver_dependency_is_persisted():
    get_step("silver").update_dependencies()
    get_step("silver").update_dependencies()

    rows = SPARK.sql("select job_id, parent, origin from fabricks.silver_dependencies").collect()

    expected = {
        ("silver.king_scd1", "bronze.king_scd1", "parser"),
        ("silver.king_and_queen_scd1", "bronze.queen_scd1", "parent"),
        ("silver.king_and_queen_scd1", "bronze.king_scd1", "parent"),
        ("silver.feature_parser", "bronze.feature_parser", "parser"),
        ("silver.queen_scd1", "bronze.queen_scd1", "parser"),
    }
    assert len(rows) == len(expected), "duplicate silver dependency rows"
    assert {(row.job_id, row.parent, row.origin) for row in rows} == {
        (get_job_id(job=job), parent, origin) for job, parent, origin in expected
    }
