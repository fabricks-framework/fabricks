from __future__ import annotations

import os
import tomllib  # type: ignore
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from fabricks.config.fabricks.base import BaseConfig


class PyprojectConfig(BaseConfig, BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    is_job_config_from_yaml: Optional[bool] = Field(None, alias="job_config_from_yaml")
    is_debugmode: Optional[bool] = Field(None, alias="debugmode")

    @classmethod
    def _from_pyproject(cls, root: Path) -> PyprojectConfig:
        path = root / "pyproject.toml"
        if not path.exists():
            return cls()  # type: ignore

        with open(path, "rb") as f:
            pyproject = tomllib.load(f)
            config = pyproject.get("tool", {}).get("fabricks", {})

        return cls(**config, root=root)

    @classmethod
    def load(cls) -> PyprojectConfig:
        path = Path(os.getcwd())

        while path is not None:
            if (path / "pyproject.toml").exists():
                break
            if path == path.parent:
                break
            path = path.parent

        return cls._from_pyproject(path)
