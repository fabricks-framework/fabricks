from __future__ import annotations

from pydantic_settings import BaseSettings

from fabricks.config.fabricks.base import BaseConfig
from fabricks.config.fabricks.environment import EnvironmentConfig
from fabricks.config.fabricks.pyproject import PyprojectConfig


class FabricksConfig(BaseConfig, BaseSettings):
    @classmethod
    def load(cls) -> FabricksConfig:
        pyproject = PyprojectConfig.load()
        environ = EnvironmentConfig()  # type: ignore

        data = {}

        if pyproject:
            dump = pyproject.model_dump(exclude_none=True)
            data.update(dump)

        # Override with environment settings
        dump = environ.model_dump(exclude_none=True)
        data.update(dump)

        return cls(**data)
