"""Job configuration models."""

from pydantic import BaseModel, ConfigDict

from fabricks.utils.path import Path


class Paths(BaseModel):
    """Runtime path references."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    to_storage: "Path"
    to_tmp: "Path"
    to_checkpoints: "Path"
    to_commits: "Path"
    to_schema: "Path"
    to_runtime: "Path"
