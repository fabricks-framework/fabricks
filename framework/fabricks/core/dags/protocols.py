from __future__ import annotations

from typing import Protocol


class BaseDagsProtocol(Protocol):
    """Satisfied by Dags — used by DagDba and DagLogger."""

    schedule_id: str


class DagsProtocol(BaseDagsProtocol, Protocol):
    """Satisfied by Dags — used by DagQuerier."""

    schedule: str
