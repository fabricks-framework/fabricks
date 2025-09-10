from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator

from fabricks.config.base import ModelBase


class BaseConfig(ModelBase):
    root: Optional[Path] = Field(None, description="Root directory")

    runtime: Optional[str] = Field(None, description="Runtime path")
    notebooks: Optional[str] = Field(None, description="Notebooks path")
    is_job_config_from_yaml: Optional[bool] = Field(None, description="Load job config from YAML")
    is_debugmode: Optional[bool] = Field(None, description="Enable debug mode")
    loglevel: Optional[str] = Field(None, description="Logging level")
    config: Optional[str] = Field(None, description="Config file path")

    @field_validator("runtime", "notebooks", "config", mode="before")
    @classmethod
    def handle_none(cls, v):
        if isinstance(v, str) and v.lower() == "none":
            return None

        return v

    @field_validator("is_job_config_from_yaml", "is_debugmode", mode="before")
    @classmethod
    def handle_bool(cls, v):
        if v is None:
            return None

        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes")

        return bool(v)

    @field_validator("loglevel", mode="before")
    @classmethod
    def handle_loglevel(cls, v):
        if v is None:
            return None

        if v.upper() not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"{v.upper()} not allowed. Use DEBUG, INFO, WARNING, ERROR or CRITICAL")

        return v.upper()

    def resolve_runtime_path(self) -> Path:
        runtime = self.runtime

        # Use environment/explicit setting if available
        if runtime is not None:
            return Path(runtime)

        # Fall back to pyproject.toml location
        if self.root is not None:
            return self.root

        # Final fallback
        raise ValueError("No pyproject.toml nor FABRICKS_RUNTIME")

    def resolve_notebooks_path(self) -> Path:
        notebooks = self.notebooks
        runtime = self.resolve_runtime_path()

        if notebooks is not None:
            if self.root is not None and not Path(notebooks).is_absolute():
                return self.root.joinpath(notebooks)

            return Path(notebooks)

        # Default to runtime/notebooks
        return runtime.joinpath("notebooks")

    def resolve_config_path(self, cluster_id: Optional[str] = None) -> Path:
        config = self.config
        runtime = self.resolve_runtime_path()

        if config is not None:
            if self.root is not None and not Path(config).is_absolute():
                return self.root.joinpath(config)

            return Path(config)

        # default to fabricks/conf.yml
        assert cluster_id is not None
        return runtime.joinpath(f"fabricks/conf.{cluster_id}.yml")
