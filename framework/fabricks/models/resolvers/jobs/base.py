from abc import abstractmethod
from typing import Any, Optional

from pydantic import BaseModel

from fabricks.models.config.fabricks import FabricksConfig
from fabricks.models.config.jobs.base import BaseJobConfig
from fabricks.models.config.runtime import RuntimeConfig
from fabricks.models.config.steps.base import BaseStepConfig


class BaseJobConfigResolver(BaseModel):
    fabricks: FabricksConfig
    runtime: RuntimeConfig
    step: BaseStepConfig
    job: BaseJobConfig

    def resolve_attribute_hierarchical(
        self,
        path: str,
        path_step: Optional[str] = None,
        path_runtime: Optional[str] = None,
        default: Optional[Any] = None,
    ):
        for config in [self.job, self.step, self.runtime]:
            if path_runtime is not None and config == self.runtime:
                path = path_runtime
            elif path_step is not None and config == self.step:
                path = path_step
            else:
                path = path

            value = config
            parts = path.split(".")
            for part in parts:
                if hasattr(value, part):
                    value = getattr(value, part)
                else:
                    value = None
                    break
            if value is not None:
                return value

        return default

    def resolve_attribute_combined(
        self,
        path: str,
        path_step: Optional[str] = None,
        path_runtime: Optional[str] = None,
        default: Optional[Any] = None,
    ):
        values = []
        for config in [self.runtime, self.step, self.job]:
            if path_runtime is not None and config == self.runtime:
                path = path_runtime
            elif path_step is not None and config == self.step:
                path = path_step
            else:
                path = path

            value = config
            parts = path.split(".")
            for part in parts:
                if hasattr(value, part):
                    value = getattr(value, part)
                else:
                    value = None
                    break
            if value is not None:
                if isinstance(value, list):
                    values.extend(value)
                else:
                    values.append(value)

        if len(values) == 0:
            return default

        return values

    def resolve_path(self, path: str) -> str:
        import re

        variables = self.runtime.variables or {}
        for key, value in variables.items():
            path = re.sub(rf"{key}", value, path)

        return path

    @classmethod
    @abstractmethod
    def from_file(cls, step: str, topic: str, item: str) -> "BaseJobConfigResolver":
        raise NotImplementedError()

    @classmethod
    def from_parts(
        cls,
        fabricks: FabricksConfig,
        runtime: RuntimeConfig,
        step: BaseStepConfig,
        job: BaseJobConfig,
    ) -> "BaseJobConfigResolver":
        return cls(fabricks=fabricks, runtime=runtime, step=step, job=job)
