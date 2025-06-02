import json
import threading
import time
from multiprocessing import Process
from typing import Any, List, Union

from azure.core.exceptions import ServiceRequestError
from databricks.sdk.runtime import dbutils, spark
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context.runtime import PATH_NOTEBOOKS
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import LOGGER
from fabricks.core.dags.run import run
from fabricks.core.jobs.base._types import TStep
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class DagProcessor(BaseDags):
    def __init__(self, schedule_id: str, schedule: str, step: Union[TStep, str], notebook: bool = True):
        self.step = get_step(step=step)
        self.schedule = schedule

        self.notebook = notebook

        self._azure_queue = None
        self._azure_table = None

        super().__init__(schedule_id=schedule_id)

    @property
    def queue(self) -> AzureQueue:
        if not self._azure_queue:
            step = self.remove_invalid_characters(str(self.step))
            self._azure_queue = AzureQueue(
                f"q{step}{self.schedule_id}", connection_string=self.get_connection_string()
            )
        return self._azure_queue

    @property
    def table(self) -> AzureTable:
        if not self._azure_table:
            self._azure_table = AzureTable(f"t{self.schedule_id}", connection_string=self.get_connection_string())
        return self._azure_table

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, ServiceRequestError)),
        reraise=True,
    )
    def query(self, data: Any) -> List[dict]:
        return self.table.query(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, ServiceRequestError)),
        reraise=True,
    )
    def upsert(self, data: Any) -> None:
        self.table.upsert(data)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, ServiceRequestError)),
        reraise=True,
    )
    def delete(self, data: Any) -> None:
        self.table.delete(data)

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
        while True:
            scheduled = self.get_scheduled()
            assert isinstance(scheduled, List)
            if len(scheduled) == 0:
                for _ in range(self.step.workers):
                    self.queue.send_sentinel()
                LOGGER.info("no more job to schedule")
                break

            else:
                sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank"))
                for s in sorted_scheduled:
                    dependencies = self.table.query(f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}'")

                    if len(dependencies) == 0:
                        s["Status"] = "waiting"
                        LOGGER.info("waiting", extra=self.extra(s))
                        self.table.upsert(s)
                        self.queue.send(s)

            time.sleep(5)

    def receive(self):
        while True:
            response = self.queue.receive()
            if response == self.queue.sentinel:
                LOGGER.info("no more job available")
                break

            elif response:
                j = json.loads(response)

                j["Status"] = "starting"
                self.table.upsert(j)
                LOGGER.info("starting", extra=self.extra(j))

                try:
                    if self.notebook:
                        dbutils.notebook.run(  # type: ignore
                            PATH_NOTEBOOKS.join("run").get_notebook_path(),  # type: ignore
                            self.step.timeouts.job,  # type: ignore
                            {
                                "schedule_id": self.schedule_id,
                                "schedule": self.schedule,  # needed to pass schedule variables to the job
                                "step": str(self.step),
                                "job_id": j.get("JobId"),
                                "job": j.get("Job"),
                            },  # type: ignore
                        )

                    else:
                        run(
                            step=str(self.step),
                            job_id=j.get("JobId"),
                            schedule_id=self.schedule_id,
                            schedule=self.schedule,
                        )

                except Exception:
                    LOGGER.warning("failed", extra={"step": str(self.step), "job": j.get("Job")})

                finally:
                    j["Status"] = "ok"
                    self.table.upsert(j)
                    LOGGER.info("ok", extra=self.extra(j))

                dependencies = self.table.query(f"PartitionKey eq 'dependencies' and ParentId eq '{j.get('JobId')}'")
                self.table.delete(dependencies)

    def get_scheduled(self, convert: bool = False):
        scheduled = self.table.query(f"PartitionKey eq 'statuses' and Status eq 'scheduled' and Step eq '{self.step}'")
        if convert:
            return spark.createDataFrame(scheduled)
        else:
            return scheduled

    def _process(self):
        scheduled = self.get_scheduled()
        assert isinstance(scheduled, List)

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
        assert isinstance(scheduled, List)

        if len(scheduled) > 0:
            LOGGER.info("start")

            p = Process(target=self._process())
            p.start()
            p.join(timeout=self.step.timeouts.step)
            p.terminate()

            self.queue.delete()

            if p.exitcode is None:
                LOGGER.critical("timeout")
                raise ValueError(f"{self.step} timed out")

            else:
                df = self.get_logs(str(self.step))
                self.write_logs(df)

                LOGGER.info("end")

        else:
            LOGGER.info("no job to schedule")

    def __str__(self) -> str:
        return f"{str(self.step)} ({self.schedule_id})"

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        if self._azure_queue:
            self._azure_queue.__exit__()
        if self._azure_table:
            self._azure_table.__exit__()

        return super().__exit__(*args, **kwargs)
