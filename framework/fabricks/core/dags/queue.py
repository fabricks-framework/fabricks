from __future__ import annotations

from typing import Any

from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class DagQueue:
    def __init__(
        self,
        step: Any,
        step_str: str,
        schedule_id: str,
        schedule: str,
        notebook: bool,
        connection_info: dict,
    ):
        self.step = step
        self._step_str = step_str
        self.schedule_id = schedule_id
        self.schedule = schedule
        self.notebook = notebook
        self._connection_info = connection_info

    def get_azure_table(self) -> AzureTable:
        return AzureTable(f"t{self.schedule_id}", **self._connection_info)

    def get_azure_queue(self) -> AzureQueue:
        return AzureQueue(f"q{self._step_str}{self.schedule_id}", **self._connection_info)

    def extra(self, d: dict) -> dict:
        return {
            "partition_key": self.schedule_id,
            "schedule": self.schedule,
            "schedule_id": self.schedule_id,
            "step": str(self.step),
            "job": d.get("Job"),
            "target": "table",
        }
