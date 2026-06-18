from __future__ import annotations

from typing import Any, Optional, Protocol

from fabricks.core.dags.config import DagConfig
from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


class BaseDagsProtocol(Protocol):
    """Satisfied by BaseDags — used by DagDba and DagLogger."""

    _config: DagConfig

    @property
    def schedule_id(self) -> str: ...


class DagsProtocol(BaseDagsProtocol, Protocol):
    """Satisfied by Dags — used by DagQuerier, DagSender, DagReceiver."""

    schedule: str
    notebook: bool
    step: Optional[Any]  # BaseStep — kept as Any to avoid importing steps (which imports jobs)

    def get_connection_info(self) -> dict: ...

    def get_azure_table(self) -> AzureTable: ...

    def get_azure_queue(self) -> AzureQueue: ...

    def extra(self, d: dict) -> dict: ...

    def remove_invalid_characters(self, s: str) -> str: ...
