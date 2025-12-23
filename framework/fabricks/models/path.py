"""Job configuration models."""

from pydantic import BaseModel, ConfigDict

from fabricks.utils.path import Path


class Paths(BaseModel):
    """Runtime path references."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage: "Path"
    tmp: "Path"
    checkpoints: "Path"
    commits: "Path"
    schema: "Path"
    runtime: "Path"
