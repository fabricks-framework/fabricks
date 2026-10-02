import json
from multiprocessing import Process
import threading
import time
from typing import Self

from azure.core.exceptions import AzureError
from pyspark.sql import DataFrame
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context import PATH_NOTEBOOKS
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.run import RunStatus, run
from fabricks.core.jobs import get_job
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import LogStatus


class DagProcessor(BaseDags):
    def __init__(self, schedule_id: str, schedule: str, step: str, notebook: bool = True) -> None:
        self.step = get_step(step=step)
        self.schedule = schedule
        self.notebook = notebook

        super().__init__(schedule_id=schedule_id)

    def get_azure_queue(self) -> AzureQueue:
        step = self.remove_invalid_characters(str(self.step))
        name = f"q{step}{self.schedule_id}"

        return AzureQueue(name, **self.get_connection_info())

    def get_azure_table(self) -> AzureTable:
        name = f"t{self.schedule_id}"
        return AzureTable(name, **self.get_connection_info())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def query(self, data: str) -> list[dict]:
        with self.get_azure_table() as azure_table:
            return azure_table.query(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def upsert(self, data: list | DataFrame | dict) -> None:
        with self.get_azure_table() as azure_table:
            azure_table.upsert(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def delete(self, data: list | DataFrame | dict) -> None:
        with self.get_azure_table() as azure_table:
            azure_table.delete(data)

    def _propagate_status(self, azure_table: AzureTable, job_id: str, status: RunStatus) -> None:
        dependencies = azure_table.query(f"PartitionKey eq 'dependencies' and ParentId eq '{job_id}'")
        for dependency in dependencies:
            dependency["Status"] = status
        azure_table.upsert(dependencies)

    def extra(self, d: dict) -> dict:
        return {
            "partition_key": self.schedule_id,
            "schedule": self.schedule,
            "schedule_id": self.schedule_id,
            "step": str(self.step),
            "job": d.get("Job"),
            "target": "table",
        }

    def send(self) -> None:
        with self.get_azure_queue() as queue, self.get_azure_table() as azure_table:
            while True:
                scheduled = self.get_scheduled(azure_table=azure_table)
                if len(scheduled) == 0:
                    for _ in range(self.step.workers):
                        queue.send_sentinel()

                    LOGGER.info("no more job to schedule", extra={"label": str(self.step)})
                    break

                sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank"))
                for s in sorted_scheduled:
                    dependencies = azure_table.query(
                        f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}' and Status eq 'pending'"
                    )

                    if len(dependencies) == 0:
                        s["Status"] = "waiting"
                        LOGGER.debug("waiting", extra=self.extra(s))
                        azure_table.upsert(s)
                        queue.send(s)

                time.sleep(5)

    def receive(self) -> None:
        with self.get_azure_queue() as queue, self.get_azure_table() as azure_table:
            while True:
                response = queue.receive()
                if response == queue.sentinel:
                    LOGGER.info("no more job to process", extra={"label": str(self.step)})
                    break

                if response:
                    j = json.loads(response)

                    j["Status"] = "starting"
                    azure_table.upsert(j)
                    LOGGER.info("start", extra=self.extra(j))

                    job = get_job(step=str(self.step), job_id=j.get("JobId"))

                    incoming = azure_table.query(f"PartitionKey eq 'dependencies' and JobId eq '{j.get('JobId')}'")
                    # Readiness gate and skip check are the only readers of incoming
                    # edges; delete now so the partition doesn't grow forever.
                    # An empty `incoming` is a safe no-op, so no guard is needed.
                    azure_table.delete(incoming)

                    if job.skip_if_stale and incoming and not any(edge.get("Status") == "ok" for edge in incoming):
                        # same pair run() logs for a stale job: DagTerminator only counts
                        # a job as not failed if it sees DONE, last_schedule.stale reads STALE
                        LOGGER.info(LogStatus.DONE, extra=self.extra(j))
                        LOGGER.info(LogStatus.STALE, extra=self.extra(j))
                        j["Status"] = "stale"
                        azure_table.upsert(j)
                        self._propagate_status(azure_table, j.get("JobId"), "stale")
                        continue

                    status: RunStatus = "stale"
                    try:
                        if self.notebook:
                            from databricks.sdk.runtime import dbutils

                            path: str = PATH_NOTEBOOKS.joinpath("run").get_notebook_path()
                            result = dbutils.notebook.run(
                                path=path,  # ty:ignore[unknown-argument]
                                timeout_seconds=self.step.timeouts.job,  # ty:ignore[unknown-argument]
                                arguments={
                                    "schedule_id": self.schedule_id,
                                    "schedule": self.schedule,  # needed to pass schedule variables to the job
                                    "step": str(self.step),
                                    "job_id": j.get("JobId"),
                                    "job": j.get("Job"),
                                },  # ty:ignore[unknown-argument]
                            )
                            status = result  # ty:ignore[invalid-assignment]

                        else:
                            status = run(job=job, schedule_id=self.schedule_id, schedule=self.schedule)

                    except Exception:
                        LOGGER.warning(LogStatus.FAILED, extra={"label": j.get("Job")})
                        status = "stale"

                    finally:
                        j["Status"] = status
                        azure_table.upsert(j)

                        LOGGER.info("end", extra=self.extra(j))
                        TABLE_LOG_HANDLER.flush()

                    self._propagate_status(azure_table, j.get("JobId"), status)

    def get_scheduled(self, azure_table: AzureTable | None = None) -> list[dict]:
        query = f"PartitionKey eq 'statuses' and Status eq 'scheduled' and Step eq '{self.step}'"
        if azure_table is not None:
            return azure_table.query(query)

        with self.get_azure_table() as at:
            return at.query(query)

    def _process(self) -> None:
        scheduled = self.get_scheduled()

        if len(scheduled) > 0:
            sender = threading.Thread(target=self.send, name=f"{str(self.step).capitalize()}Sender", args=())
            sender.start()

            receivers = []
            for i in range(self.step.workers):
                receiver = threading.Thread(
                    target=self.receive, name=f"{str(self.step).capitalize()}Receiver{i}", args=()
                )
                receiver.start()
                receivers.append(receiver)

            sender.join()
            for receiver in receivers:
                receiver.join()

    def process(self) -> None:
        scheduled = self.get_scheduled()

        if len(scheduled) > 0:
            LOGGER.info("start", extra={"label": str(self.step)})

            p = Process(target=self._process)
            p.start()
            p.join(timeout=self.step.timeouts.step)
            p.terminate()

            try:
                with self.get_azure_queue() as queue:
                    queue.delete()
            except AzureError:
                # Queue may already be deleted or not exist
                pass

            if p.exitcode is None:
                LOGGER.critical("timeout", extra={"label": str(self.step)})
                raise ValueError(f"{self.step} timed out")

            df = self.get_logs(str(self.step))
            self.write_logs(df)

            LOGGER.info("end", extra={"label": str(self.step)})

        else:
            LOGGER.info("no job to schedule", extra={"label": str(self.step)})

    def __str__(self) -> str:
        return f"{self.step!s} ({self.schedule_id})"

    def __enter__(self) -> Self:
        return super().__enter__()

    def __exit__(self, *args: object, **kwargs: object) -> None:
        # Each thread manages its own queue/table lifecycle via context managers
        return super().__exit__(*args, **kwargs)
