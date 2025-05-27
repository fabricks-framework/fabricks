import time
from typing import Optional, Tuple
from uuid import uuid4

from pyspark.sql import DataFrame

from fabricks.context import SPARK
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import TABLE_LOG_HANDLER
from fabricks.utils.azure_queue import AzureQueue


class DagGenerator(BaseDags):
    def __init__(self, schedule: str):
        self.schedule = schedule
        schedule_id = str(uuid4().hex)
        super().__init__(schedule_id=schedule_id)

    def get_jobs(self) -> DataFrame:
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
              '{self.schedule_id}' as ScheduleId,
              '{self.schedule}' as Schedule,
              j.job_id::string as RowKey,
              j.step as Step,
              j.job_id as JobId,
              j.job as Job,
              'scheduled' as `Status`,
              max(median_duration) as `MedianDuration`, 
              dense_rank() over (order by max(median_duration) desc) as Rank
            from
              fabricks.jobs j
              inner join fabricks.{self.schedule}_schedule v on j.job_id = v.job_id
              left join logs l on j.job_id = l.job_id
            group by all
            """
        )

    def get_dependencies(self, job_df: Optional[DataFrame] = None) -> DataFrame:
        if job_df is None:
            job_df = self.get_jobs()

        return SPARK.sql(
            """
            select
              'dependencies' as PartitionKey, 
              d.dependency_id::string as RowKey,
              {schedule_id} as ScheduleId,
              {schedule} as Schedule,
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
            schedule=self.schedule,
            schedule_id=self.schedule_id,
        )

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

    def generate(self) -> Tuple[str, DataFrame, DataFrame]:
        job_df = self.get_jobs()
        deps_df = self.get_dependencies(job_df)
        step_df = self.get_steps(job_df)

        table = self.get_table()

        table.create_if_not_exists()
        table.truncate_all_partitions()

        table.upsert(job_df)
        table.upsert(deps_df)

        df = SPARK.sql(
            """
            select
              ScheduleId as PartitionKey,
              ScheduleId,
              `Schedule`,
              Step,
              Job,
              JobId,
              date_format(current_timestamp(), 'dd/MM/yy HH:mm:ss') as Created,
              'INFO' as `Level`,
              `Status` as `Message`,
              from_json(null, 'type STRING, message STRING, traceback STRING') as Exception,
              md5(array_join(array(ScheduleId, `Schedule`, Step, Job, JobId, Created,  `Level`, `Message`, -1), "*")) as RowKey
            from
              {df}        
            """,
            df=job_df,
        )

        TABLE_LOG_HANDLER.table.upsert(df)

        cs = self.get_connection_string()
        for row in step_df.collect():
            step = self.remove_invalid_characters(row.Step)

            with AzureQueue(f"q{step}{self.schedule_id}", connection_string=cs) as queue:
                queue.create_if_not_exists()
                queue.clear()

        time.sleep(60)

        return self.schedule_id, job_df, deps_df
