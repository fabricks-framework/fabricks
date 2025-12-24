from typing import Any, List, Optional

from pydantic import BaseModel

from fabricks.models.common import TStep


class ScheduleOptions(BaseModel):
    steps: Optional[List[TStep]] = None
    tag: Optional[str] = None
    view: Optional[str] = None
    variables: Optional[dict[Any, Any]] = None


class Schedule(BaseModel):
    name: str
    options: ScheduleOptions
