from fabricks.context import SPARK
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


class DagTerminator(BaseDags):
    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id
        super().__init__(schedule_id=schedule_id)

    def terminate(self):
        logs_df = self.get_logs()
        self.write_logs(logs_df)

        not_done_df = SPARK.sql(
            """
            with base as (
              select
                job,
                not array_contains(collect_list(status), 'done') as not_done
              from
                {logs}
              group by
                job
            )
            select
              *
            from
              base
            where
              not_done
            """,
            logs=logs_df,
        )

        rows = not_done_df.collect()
        for row in rows:
            LOGGER.error(f"{row['job']} failed")

        TABLE_LOG_HANDLER.table.truncate_partition(self.schedule_id)

        table = self.get_table()
        table.drop()

        if rows:
            raise ValueError(f"{len(rows)} job(s) failed")
