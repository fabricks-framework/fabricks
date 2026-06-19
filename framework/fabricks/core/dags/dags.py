import threading
import time
from multiprocessing import Process
from typing import Any, List, Optional, Tuple
from uuid import uuid4

from azure.core.exceptions import AzureError
from pyspark.sql import DataFrame
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context import SPARK
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.delegates.querier import DagQuerier
from fabricks.core.dags.delegates.receiver import DagReceiver
from fabricks.core.dags.delegates.sender import DagSender
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.queue import DagQueue
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class Dags(BaseDags):
    def __init__(self, schedule: Optional[str] = None, schedule_id: Optional[str] = None):
        self.schedule = schedule or ""
        self.step = None
        self.notebook = True
        super().__init__(schedule_id=schedule_id or str(uuid4().hex))
        self._querier = DagQuerier(self)
        self._sender = DagSender()
        self._receiver = DagReceiver()

    # --- DagQuerier shims ---

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def query(self, data: Any) -> List[dict]:
        with self.get_azure_table() as azure_table:
            return azure_table.query(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def upsert(self, data: Any) -> None:
        with self.get_azure_table() as azure_table:
            azure_table.upsert(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def delete(self, data: Any) -> None:
        with self.get_azure_table() as azure_table:
            azure_table.delete(data)

    # --- DagSender shims ---

    def get_scheduled(self, azure_table: Optional[AzureTable] = None) -> list[dict]:
        return self._sender.get_scheduled(self._make_queue_ctx(), azure_table)

    def send(self):
        return self._sender.send(self._make_queue_ctx())

    # --- DagReceiver shim ---

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
