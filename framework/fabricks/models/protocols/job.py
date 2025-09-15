from abc import ABC

from fabricks.models.config.resolver import BaseConfigResolver


class IJob(ABC):
    config: BaseConfigResolver

    step: str
    topic: str
    item: str

    job_id: str
    job: str

    def run(self) -> None: ...

    def drop(self) -> None: ...

    def create(self) -> None: ...

    def get_data(self) -> None: ...

    @classmethod
    def from_config(cls, config: BaseConfigResolver) -> "IJob": ...

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str) -> "IJob": ...
