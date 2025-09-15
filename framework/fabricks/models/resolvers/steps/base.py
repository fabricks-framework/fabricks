from abc import abstractmethod
from typing import List

from pydantic import BaseModel

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.jobs.base import BaseJobConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.base import BaseStepConfig


class BaseStepConfigResolver(BaseModel):
    fabricks: FabricksConfig
    runtime: RuntimeConfig
    step: BaseStepConfig
    jobs: List[BaseJobConfig]

    @classmethod
    @abstractmethod
    def from_file(cls, step: str) -> "BaseStepConfigResolver":
        raise NotImplementedError()

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: BaseStepConfig,
        jobs: List[BaseJobConfig],
    ) -> "BaseStepConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, jobs=jobs)
