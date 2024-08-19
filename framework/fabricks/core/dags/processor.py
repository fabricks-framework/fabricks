import json
import threading
import time
from multiprocessing import Process
from typing import List, Union

from databricks.sdk.runtime import dbutils, spark

from fabricks.context.runtime import PATH_NOTEBOOKS
from fabricks.core.dags.base import BaseDags
from fabricks.core.dags.log import DagsLogger
from fabricks.core.jobs.base.types import TStep
from fabricks.core.steps.get_step import get_step
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class DagProcessor(BaseDags):
    def __init__(self, schedule_id: str, schedule: str, step: Union[TStep, str]):
        self.step = get_step(step=step)
        self.schedule = schedule
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
                DagsLogger.info("🎉 (no more job to schedule)")
                break

            else:
                sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank"))
                for s in sorted_scheduled:
                    dependencies = self.table.query(f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}'")
                    if len(dependencies) == 0:
                        s["Status"] = "waiting"
                        DagsLogger.info("waiting", extra=self.extra(s))
                        self.table.upsert(s)
                        self.queue.send(s)

            time.sleep(5)

    def receive(self):
        while True:
            response = self.queue.receive()
            if response == self.queue.sentinel:
                DagsLogger.info("💤 (no more job available)")
                break
            elif response:
                j = json.loads(response)

                j["Status"] = "starting"
                self.table.upsert(j)
                DagsLogger.info("starting", extra=self.extra(j))

                try:
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
                    )  # type: ignore

                except Exception:
                    DagsLogger.warning("🤯 (failed)", extra={"step": str(self.step), "job": j.get("Job")})

                finally:
                    j["Status"] = "ok"
                    self.table.upsert(j)
                    DagsLogger.info("ok", extra=self.extra(j))

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
            DagsLogger.info("🏎️ (start)")

            p = Process(target=self._process())
            p.start()
            p.join(timeout=self.step.timeouts.step)
            p.terminate()

            self.queue.delete()

            if p.exitcode is None:
                DagsLogger.critical("💥 (timeout)")
                raise ValueError(f"{self.step} timed out")

            else:
                df = self.get_logs(str(self.step))
                self.write_logs(df)

                DagsLogger.info("🏁 (end)")

        else:
            DagsLogger.info("no job to schedule (🏖️)")

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
