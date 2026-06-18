from __future__ import annotations

from typing import Optional

from fabricks.utils.azure_table import AzureTable


class DagConfig:
    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id
        self._connection_info: Optional[dict] = None
        self._table: Optional[AzureTable] = None
