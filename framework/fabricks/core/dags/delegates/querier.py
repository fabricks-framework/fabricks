from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.context import SPARK

if TYPE_CHECKING:
    from fabricks.core.dags.dags import Dags


class DagQuerier:
    def __init__(self, dags: Dags):
        self._dags = dags

    def get_jobs(self) -> DataFrame:
        g = self._dags
        return SPARK.sql(
            f"""
            with logs as (
              select
                l.job_id,
                median(l.duration) as median_duration
              from
                fabricks.logs_pivot l
              where
                true
                and duration is not null
                and date_diff(day, l.start_time , current_date) < 10
              group by
                l.job_id
            )
            select
              'statuses' as PartitionKey,
              '{g.schedule_id}' as ScheduleId,
              '{g.schedule}' as Schedule,
              j.job_id::string as RowKey,
              j.step as Step,
              j.job_id as JobId,
              j.job as Job,
              'scheduled' as `Status`,
              max(median_duration) as `MedianDuration`,
              dense_rank() over (order by max(median_duration) desc) as Rank
            from
              fabricks.jobs j
              inner join fabricks.{g.schedule}_schedule v on j.job_id = v.job_id
              left join logs l on j.job_id = l.job_id
            group by all
            """
        )

    def get_dependencies(self, job_df: Optional[DataFrame] = None) -> DataFrame:
        g = self._dags
        if job_df is None:
            job_df = self.get_jobs()

        df = SPARK.sql(
            """
            select
              'dependencies' as PartitionKey,
              d.dependency_id :: string as RowKey,
              d.dependency_id as DependencyId,
              j.Step as Step,
              j.Job as Job,
              j.JobId as JobId,
              p.Step as ParentStep,
              p.Job as Parent,
              p.JobId as ParentId
            from
              fabricks.dependencies d
              inner join {job} j on d.job_id = j.JobId
              inner join {job} p on d.parent_id = p.JobId
            where
              true
              and d.parent_id is not null
              and not d.job_id = d.parent_id
              and not exists (
                select 1
                from
                  fabricks.dependencies_circular dc
                where
                  true
                  and d.job_id = dc.job_id
                  and d.parent_id = dc.parent_id

              )
            group by all
            """,
            job=job_df,
        )
        df = df.withColumn("ScheduleId", lit(g.schedule_id))
        return df.withColumn("Schedule", lit(g.schedule))

    def get_steps(self, job_df: Optional[DataFrame] = None) -> DataFrame:
        if job_df is None:
            job_df = self.get_jobs()

        return SPARK.sql(
            """
            select
              Step
            from
              {job}
            group by
              Step
            """,
            job=job_df,
        )
