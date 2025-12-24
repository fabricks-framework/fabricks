"""Job configuration models."""

from dataclasses import dataclass

from fabricks.utils.path import Path


@dataclass(frozen=True)
class Paths:
    """Runtime path references."""

    to_storage: "Path"
    to_tmp: "Path"
    to_checkpoints: "Path"
    to_commits: "Path"
    to_schema: "Path"
    to_runtime: "Path"
