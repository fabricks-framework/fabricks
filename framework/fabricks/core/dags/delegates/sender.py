from __future__ import annotations

import time
from typing import Any, Optional

from fabricks.core.dags.log import LOGGER
from fabricks.core.dags.queue import DagQueue
from fabricks.utils.azure_table import AzureTable


class DagSender:
    def get_scheduled(self, ctx: DagQueue, azure_table: Optional[AzureTable] = None) -> list[dict[str, Any]]:
        query = f"PartitionKey eq 'statuses' and Status eq 'scheduled' and Step eq '{ctx.step}'"
        if azure_table is not None:
            return azure_table.query(query)
        with ctx.get_azure_table() as at:
            return at.query(query)

    def send(self, ctx: DagQueue):
        assert ctx.step is not None
        with ctx.get_azure_queue() as queue, ctx.get_azure_table() as azure_table:
            while True:
                scheduled = self.get_scheduled(ctx, azure_table=azure_table)
                if len(scheduled) == 0:
                    for _ in range(ctx.step.workers):
                        queue.send_sentinel()
                    LOGGER.info("no more job to schedule", extra={"label": str(ctx.step)})
                    break

                sorted_scheduled = sorted(scheduled, key=lambda x: x.get("Rank") or 0)
                for s in sorted_scheduled:
                    dependencies = azure_table.query(f"PartitionKey eq 'dependencies' and JobId eq '{s.get('JobId')}'")
                    if len(dependencies) == 0:
                        s["Status"] = "waiting"
                        LOGGER.debug("waiting", extra=ctx.extra(s))
                        azure_table.upsert(s)
                        queue.send(s)

                time.sleep(5)
