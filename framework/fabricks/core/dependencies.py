from pyspark.sql.types import Row

from fabricks.context import SPARK, Steps
from fabricks.context.log import DEFAULT_LOGGER


def check_dependencies(step: str, job_id: str | None = None) -> list[Row]:
    """Edges of `step` (or of one job) whose parent is not a Fabricks job, each logged as a warning.

    The scheduler drops such edges, so the job runs without waiting for that parent (#264).
    """
    # a refresh may run before update_configurations has registered the step's jobs table
    if not all(SPARK.catalog.tableExists(f"fabricks.{step}_{t}") for t in ("jobs", "dependencies")):
        return []

    jobs = [f"select job_id from fabricks.{s}_jobs" for s in Steps if SPARK.catalog.tableExists(f"fabricks.{s}_jobs")]

    where_job = f"and d.job_id = '{job_id}'" if job_id else ""
    rows = SPARK.sql(
        f"""
        select concat(j.step, '.', j.topic, '_', j.item) as job, d.parent, d.origin
        from fabricks.{step}_dependencies d
        left join fabricks.{step}_jobs j on j.job_id = d.job_id
        where d.parent_id not in ({" union all ".join(jobs)}) {where_job}
        order by job, d.parent
        """
    ).collect()

    for r in rows:
        DEFAULT_LOGGER.warning(
            f"{r.parent} ({r.origin}) is not a fabricks job, the scheduler ignores this dependency",
            extra={"label": r.job},
        )
    return rows
