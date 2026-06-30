from dataclasses import dataclass
from typing import Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict

from fabricks.models import JobDependency

Modes = Literal["parallel", "sequential"]


@dataclass
class Timeouts:
    job: int
    step: int


class JobResult(BaseModel):
    """Outcome of a per-job worker (create / register / get dependencies)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    job: str
    job_id: Optional[str] = None
    error: Optional[Exception] = None
    dependencies: Optional[Sequence[JobDependency]] = None
