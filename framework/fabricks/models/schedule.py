from typing import List

from pydantic import BaseModel

from fabricks.models.common import TStep


class ScheduleOptions(BaseModel):
    steps: List[TStep] | None = None
    tag: str | None = None
    view: str | None = None
    variables: dict[str, str | bool] | None = None


class Schedule(BaseModel):
    name: str
    options: ScheduleOptions
