from pydantic import BaseModel


class ScheduleOptions(BaseModel):
    steps: list[str] | None = None
    tag: str | None = None
    view: str | None = None
    variables: dict[str, str | bool | int] | None = None


class Schedule(BaseModel):
    name: str
    options: ScheduleOptions
