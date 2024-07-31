from typing import Optional

from databricks.sdk.runtime import spark
from pyspark.sql import Row

from fabricks.core.jobs.get_job import get_job
from fabricks.utils.helpers import run_in_parallel


def optimize(schedule_id: Optional[str] = None):
    """
    Cleans the Fabricks jobs by vacuuming and optimizing the tables.

    Args:
        schedule_id (Optional[str]): The schedule ID to filter the jobs. If None, all jobs will be cleaned.

    Returns:
        None
    """
    if schedule_id is not None:
        df = spark.sql(
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
        df = spark.sql("select * from fabricks.jobs where not mode = 'memory'")

    def _optimize(row: Row):
        job = get_job(step=row["step"], job_id=row["job_id"])
        job.optimize()

    run_in_parallel(_optimize, df, 16)
