from typing import Optional

from pyspark.sql.types import Row

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.get_job import get_job
from fabricks.utils.helpers import run_in_parallel


def optimize(schedule_id: Optional[str] = None, workers: Optional[int] = 16):
    """
    Cleans the Fabricks jobs by vacuuming and optimizing the tables.

    Args:
        schedule_id (Optional[str]): The schedule ID to filter the jobs. If None, all jobs will be cleaned.

    Returns:
        None
    """
    if schedule_id is not None:
        df = SPARK.sql(
            f"""
            select
              j.step,
              j.job_id
            from
              fabricks.logs l
              inner join fabricks.jobs j on l.job_id = j.job_id
            where 
              true
              and not j.mode = 'memory'
              and l.schedule_id = '{schedule_id}'
            group by
              j.step,
              j.job_id
            """
        )
    else:
        df = SPARK.sql("select * from fabricks.jobs where not mode = 'memory'")

    errors = []

    def _optimize(row: Row):
        job = get_job(step=row["step"], job_id=row["job_id"])
        try:
            job.optimize()
        except Exception as e:
            errors.append((str(job), e))

    if workers is None:
        workers = 16

    run_in_parallel(_optimize, df, workers)
    for e in errors:
        DEFAULT_LOGGER.exception("could not optimize job", extra={"job": e[0]}, exc_info=e[1])

    if len(errors) > 0:
        raise ValueError(f"Failed to optimize {len(errors)} job(s)")
