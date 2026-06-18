from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame

from fabricks.core.dags.config import DagConfig
from fabricks.core.dags.delegates.dba import DagDba
from fabricks.core.dags.delegates.logger import DagLogger
from fabricks.utils.azure_table import AzureTable


class BaseDags:
    def __init__(self, schedule_id: str):
        self._config = DagConfig(schedule_id)
        self._dba = DagDba(self)
        self._logger = DagLogger(self)

    @property
    def schedule_id(self) -> str:
        return self._config.schedule_id

    # DagStore shims

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

    # DagLogger shims

    def get_logs(self, step: Optional[str] = None) -> DataFrame:
        return self._logger.get_logs(step)

    def write_logs(self, df: DataFrame):
        return self._logger.write_logs(df)

    def remove_invalid_characters(self, s: str) -> str:
        return self._logger.remove_invalid_characters(s)
