from abc import ABC

from fabricks.models.resolvers.steps import BaseStepConfigResolver


class IStep(ABC):
    config: BaseStepConfigResolver

    step: str
    topic: str
    item: str

    job_id: str
    job: str

    def update(self) -> None: ...

    def create_db_objects(self) -> None: ...

    @classmethod
    def from_config(cls, config: BaseStepConfigResolver) -> "IStep": ...

    @classmethod
    def from_step(cls, step: str) -> "IStep": ...
