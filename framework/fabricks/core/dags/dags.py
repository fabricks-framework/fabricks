import threading
import time
from multiprocessing import Process
from typing import Optional, Tuple
from uuid import uuid4

from azure.core.exceptions import AzureError
from pyspark.sql import DataFrame

from fabricks.context import SPARK
from fabricks.core.dags.delegates.dba import DagDba
from fabricks.core.dags.delegates.logger import DagLogger
from fabricks.core.dags.delegates.querier import DagQuerier
from fabricks.core.dags.delegates.receiver import DagReceiver
from fabricks.core.dags.delegates.sender import DagSender
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.queue import DagQueue
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class Dags:
    def __init__(self, schedule: Optional[str] = None, schedule_id: Optional[str] = None):
        self.schedule = schedule or ""
        self.schedule_id = schedule_id or str(uuid4().hex)
        self.step = None
        self.notebook = True
        self._dba = DagDba(self)
        self._logger = DagLogger(self)
        self._querier = DagQuerier(self)
        self._sender = DagSender()
        self._receiver = DagReceiver()

    # --- DagDba ---

    @property
    def storage_account(self) -> str:
        return self._dba.storage_account

    def get_connection_info(self) -> dict:
        return self._dba.get_connection_info()

    def get_table(self) -> AzureTable:
        return self._dba.get_table()

    def __enter__(self):
        return self._dba.__enter__()

    def __exit__(self, *args, **kwargs):
        return self._dba.__exit__(*args, **kwargs)

    # --- DagLogger ---

    def get_logs(self, step: Optional[str] = None) -> DataFrame:
        return self._logger.get_logs(step)

    def write_logs(self, df: DataFrame):
        return self._logger.write_logs(df)

    def remove_invalid_characters(self, s: str) -> str:
        return self._logger.remove_invalid_characters(s)

    # --- DagQuerier ---

    def get_jobs(self) -> DataFrame:
        return self._querier.get_jobs()

    def get_dependencies(self, job_df: Optional[DataFrame] = None) -> DataFrame:
        return self._querier.get_dependencies(job_df)

    def get_steps(self, job_df: Optional[DataFrame] = None) -> DataFrame:
        return self._querier.get_steps(job_df)

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
              md5(array_join(array(ScheduleId, `Schedule`, Step, Job, JobId, Created,  `Level`, `Message`, '-1'), "*")) as RowKey
            from
              {df}
            """,
            df=job_df,
        )

        TABLE_LOG_HANDLER.table.upsert(df)

        cs = self.get_connection_info()
        rows = step_df.collect()
        for row in rows:
            step = self.remove_invalid_characters(row.Step)
            with AzureQueue(f"q{step}{self.schedule_id}", **dict(cs)) as queue:
                queue.create_if_not_exists()
                queue.clear()

        # wait for queues to be ready before starting the dag
        time.sleep(60)

        return self.schedule_id, job_df, deps_df

    def _make_queue_ctx(self) -> DagQueue:
        assert self.step is not None
        return DagQueue(
            step=self.step,
            step_str=self.remove_invalid_characters(str(self.step)),
            schedule_id=self.schedule_id,
            schedule=self.schedule,
            notebook=self.notebook,
            connection_info=self.get_connection_info(),
        )

    # --- DagSender ---

    def get_scheduled(self, azure_table: Optional[AzureTable] = None) -> list[dict]:
        return self._sender.get_scheduled(self._make_queue_ctx(), azure_table)

    def send(self):
        return self._sender.send(self._make_queue_ctx())

    # --- DagReceiver ---

    def receive(self):
        return self._receiver.receive(self._make_queue_ctx())

    def _process(self):
        ctx = self._make_queue_ctx()
        scheduled = self._sender.get_scheduled(ctx)
        if len(scheduled) > 0:
            sender = threading.Thread(
                target=self._sender.send,
                name=f"{str(ctx.step).capitalize()}Sender",
                args=(ctx,),
            )
            sender.start()

            receivers = []
            for i in range(ctx.step.workers):
                receiver = threading.Thread(
                    target=self._receiver.receive,
                    name=f"{str(ctx.step).capitalize()}Receiver{i}",
                    args=(ctx,),
                )
                receiver.start()
                receivers.append(receiver)

            sender.join()
            for receiver in receivers:
                receiver.join()

    def process(self, step: str, notebook: bool = True):
        self.step = get_step(step=step)
        self.notebook = notebook
        ctx = self._make_queue_ctx()

        scheduled = self._sender.get_scheduled(ctx)
        if len(scheduled) > 0:
            LOGGER.info("start", extra={"label": str(ctx.step)})

            p = Process(target=self._process)
            p.start()
            p.join(timeout=ctx.step.timeouts.step)
            p.terminate()

            try:
                with ctx.get_azure_queue() as queue:
                    queue.delete()
            except AzureError:
                pass

            if p.exitcode is None:
                LOGGER.critical("timeout", extra={"label": str(ctx.step)})
                raise ValueError(f"{ctx.step} timed out")
            else:
                df = self.get_logs(str(ctx.step))
                self.write_logs(df)
                LOGGER.info("end", extra={"label": str(ctx.step)})
        else:
            LOGGER.info("no job to schedule", extra={"label": str(ctx.step)})

    # --- terminate ---

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

    def __str__(self) -> str:
        return f"{self.schedule} ({self.schedule_id})"
