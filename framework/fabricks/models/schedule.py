from pydantic import BaseModel, ConfigDict

from fabricks.models.config import config


class ScheduleOptions(BaseModel):
    """Options for scheduling a notebook run."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    steps: list[str] | None = None
    tag: str | None = None
    view: str | None = None
    variables: dict[str, str | bool | int] | None = None


class Schedule(BaseModel):
    """Schedule model representing a notebook schedule."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    name: str
    options: ScheduleOptions
