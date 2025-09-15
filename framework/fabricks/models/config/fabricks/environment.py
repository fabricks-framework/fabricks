from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict

from fabricks.models.config.fabricks.base import BaseConfig


class EnvironmentConfig(BaseConfig, BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FABRICKS_", case_sensitive=False, extra="ignore")
