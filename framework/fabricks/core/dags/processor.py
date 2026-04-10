import json
import threading
import time
from multiprocessing import Process
from typing import Any, List, Optional

from azure.core.exceptions import AzureError
from databricks.sdk.runtime import dbutils
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context import PATH_NOTEBOOKS
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.dags.run import run
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class DagProcessor(BaseDags):
    def __init__(self, schedule_id: str, schedule: str, step: str, notebook: bool = True):
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

    def extra(self, d: dict) -> dict:
        return {
            "partition_key": self.schedule_id,
            "schedule": self.schedule,
            "schedule_id": self.schedule_id,
            "step": str(self.step),
            "job": d.get("Job"),
            "target": "table",
        }

    def send(self):
        with self.get_azure_queue() as queue, self.get_azure_table() as azure_table:
            while True:
                scheduled = self.get_scheduled(azure_table=azure_table)
                if len(scheduled) == 0:
                    for _ in range(self.step.workers):
                        queue.send_sentinel()

                    LOGGER.info("no more job to schedule", extra={"label": str(self.step)})
                    break

                else:
                    sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank"))
                    for s in sorted_scheduled:
                        dependencies = azure_table.query(
                            f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}'"
                        )

                        if len(dependencies) == 0:
                            s["Status"] = "waiting"
                            LOGGER.debug("waiting", extra=self.extra(s))
                            azure_table.upsert(s)
                            queue.send(s)

                time.sleep(5)

    def receive(self):
        with self.get_azure_queue() as queue, self.get_azure_table() as azure_table:
            while True:
                response = queue.receive()
                if response == queue.sentinel:
                    LOGGER.info("no more job to process", extra={"label": str(self.step)})
                    break

                elif response:
                    j = json.loads(response)

                    j["Status"] = "starting"
                    azure_table.upsert(j)
                    LOGGER.info("start", extra=self.extra(j))

                    try:
                        if self.notebook:
                            path: str = PATH_NOTEBOOKS.joinpath("run").get_notebook_path()
                            dbutils.notebook.run(
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

                        else:
                            run(
                                step=str(self.step),
                                job_id=j.get("JobId"),
                                schedule_id=self.schedule_id,
                                schedule=self.schedule,
                            )

                    except Exception:
                        LOGGER.warning("fail", extra={"label": j.get("Job")})

                    finally:
                        j["Status"] = "ok"
                        azure_table.upsert(j)

                        LOGGER.info("end", extra=self.extra(j))
                        TABLE_LOG_HANDLER.flush()

                    dependencies = azure_table.query(
                        f"PartitionKey eq 'dependencies' and ParentId eq '{j.get('JobId')}'"
                    )
                    azure_table.delete(dependencies)

    def get_scheduled(self, azure_table: Optional[AzureTable] = None) -> list[dict]:
        query = f"PartitionKey eq 'statuses' and Status eq 'scheduled' and Step eq '{self.step}'"
        if azure_table is not None:
            return azure_table.query(query)
        with self.get_azure_table() as at:
            return at.query(query)

    def _process(self):
        scheduled = self.get_scheduled()

        if len(scheduled) > 0:
            sender = threading.Thread(
                target=self.send,
                name=f"{str(self.step).capitalize()}Sender",
                args=(),
            )
            sender.start()

            receivers = []
            for i in range(self.step.workers):
                receiver = threading.Thread(
                    target=self.receive,
                    name=f"{str(self.step).capitalize()}Receiver{i}",
                    args=(),
                )
                receiver.start()
                receivers.append(receiver)

            sender.join()
            for receiver in receivers:
                receiver.join()

    def process(self):
        scheduled = self.get_scheduled()

        if len(scheduled) > 0:
            LOGGER.info("start", extra={"label": str(self.step)})

            p = Process(target=self._process())
            p.start()
            p.join(timeout=self.step.timeouts.step)
            p.terminate()

            with self.get_azure_queue() as queue:
                queue.delete()

            if p.exitcode is None:
                LOGGER.critical("timeout", extra={"label": str(self.step)})
                raise ValueError(f"{self.step} timed out")

            else:
                df = self.get_logs(str(self.step))
                self.write_logs(df)

                LOGGER.info("end", extra={"label": str(self.step)})

        else:
            LOGGER.info("no job to schedule", extra={"label": str(self.step)})

    def __str__(self) -> str:
        return f"{str(self.step)} ({self.schedule_id})"

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        # Each thread manages its own queue/table lifecycle via context managers
        return super().__exit__(*args, **kwargs)
