from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

from fabricks.core.dags.log import LOGGER
from fabricks.utils.azure_table import AzureTable

if TYPE_CHECKING:
    from fabricks.core.dags.dags import Dags


class DagSender:
    def __init__(self, dags: Dags):
        self._dags = dags

    def get_scheduled(self, azure_table: Optional[AzureTable] = None) -> list[dict]:
        dags = self._dags
        query = f"PartitionKey eq 'statuses' and Status eq 'scheduled' and Step eq '{dags.step}'"
        if azure_table is not None:
            return azure_table.query(query)
        with dags.get_azure_table() as at:
            return at.query(query)

    def send(self):
        dags = self._dags
        assert dags.step is not None
        with dags.get_azure_queue() as queue, dags.get_azure_table() as azure_table:
            while True:
                scheduled = self.get_scheduled(azure_table=azure_table)
                if len(scheduled) == 0:
                    for _ in range(dags.step.workers):
                        queue.send_sentinel()
                    LOGGER.info("no more job to schedule", extra={"label": str(dags.step)})
                    break

                sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank"))
                for s in sorted_scheduled:
                    dependencies = azure_table.query(f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}'")
                    if len(dependencies) == 0:
                        s["Status"] = "waiting"
                        LOGGER.debug("waiting", extra=dags.extra(s))
                        azure_table.upsert(s)
                        queue.send(s)

                time.sleep(5)
