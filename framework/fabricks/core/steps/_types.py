from dataclasses import dataclass
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict

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
    dependencies: Optional[List[Any]] = None
