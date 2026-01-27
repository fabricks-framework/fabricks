import logging
import os
import pathlib
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__file__)


class ConfigOptions(BaseSettings):
    """Main configuration options for Fabricks framework."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    config: str = Field(alias="FABRICKS_CONFIG", default="none")
    runtime: str = Field(alias="FABRICKS_RUNTIME", default="none")
    notebooks: str = Field(alias="FABRICKS_NOTEBOOKS", default="none")
    job_config_from_yaml: bool = Field(alias="FABRICKS_IS_JOB_CONFIG_FROM_YAML", default=False)
    debugmode: bool = Field(alias="FABRICKS_IS_DEBUGMODE", default=False)
    funmode: bool = Field(alias="FABRICKS_IS_FUNMODE", default=False)
    devmode: bool = Field(alias="FABRICKS_IS_DEVMODE", default=False)
    loglevel: int = Field(alias="FABRICKS_LOGLEVEL", default=20)

    @field_validator("job_config_from_yaml", "debugmode", "funmode", "devmode", mode="before")
    @classmethod
    def validate_bool(cls, v):
        """Convert string representations of boolean values to bool."""
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            if v.lower() in ("true", "1", "yes"):
                return True
            elif v.lower() in ("false", "0", "no"):
                return False
        return v

    @field_validator("loglevel", mode="before")
    @classmethod
    def validate_loglevel(cls, v):
        """Validate log level."""
        if isinstance(v, str):
            levels = {
                "DEBUG": logging.DEBUG,
                "INFO": logging.INFO,
                "WARNING": logging.WARNING,
                "ERROR": logging.ERROR,
                "CRITICAL": logging.CRITICAL,
            }
            v_upper = v.upper()
            if v_upper in levels:
                return levels[v_upper]

            return logging.INFO  # Default log level

        return v

    @field_validator("notebooks", mode="before")
    @classmethod
    def validate_notebooks(cls, v):
        """Set default notebooks path if not provided."""
        if not v or v == "none":
            return "runtime/notebooks"
        return v

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, file_secret_settings):
        # Load from pyproject.toml
        def pyproject_settings(base: Path):
            pyproject_path = base / "pyproject.toml"
            if pyproject_path.exists():
                import sys

                if sys.version_info >= (3, 11):
                    import tomllib
                else:
                    import tomli as tomllib  # type: ignore

                logger.debug(f"Loading configuration from {pyproject_path}")
                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)

                return data.get("tool", {}).get("fabricks", {})

            return None

        # Load from fabricksconfig.json
        def json_settings(base: Path):
            json_path = base / "fabricksconfig.json"
            if json_path.exists():
                import json

                logger.debug(f"Loading configuration from {json_path}")
                with open(json_path, "r") as f:
                    data = json.load(f)

                return data

            return None

        def hierarchical_file_settings():
            path = pathlib.Path(os.getcwd())
            data = None

            while not data:
                data = json_settings(path)
                if data:
                    break

                data = pyproject_settings(path)
                if data:
                    break

                if path == path.parent:
                    break

                path = path.parent

            return data

        # Order: env vars > file > defaults
        return (env_settings, hierarchical_file_settings, init_settings, file_secret_settings)
