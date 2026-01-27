import logging
import os
import pathlib
from pathlib import Path

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class HierarchicalFileSettingsSource(PydanticBaseSettingsSource):
    """Custom settings source for hierarchical file configuration."""

    def get_field_value(self, field):
        # Not used in this implementation
        return None, None, False

    def __call__(self):
        """Load settings from hierarchical file search."""
        data = self._load_hierarchical_file()
        return data

    def _load_hierarchical_file(self):
        """Search up directory hierarchy for configuration files."""

        def pyproject_settings(base: Path):
            pyproject_path = base / "pyproject.toml"
            if pyproject_path.exists():
                import sys

                if sys.version_info >= (3, 11):
                    import tomllib
                else:
                    import tomli as tomllib  # type: ignore

                with open(pyproject_path, "rb") as f:
                    data = tomllib.load(f)

                return data.get("tool", {}).get("fabricks", {})

            return None

        def json_settings(base: Path):
            json_path = base / "fabricksconfig.json"
            if json_path.exists():
                import json

                with open(json_path, "r") as f:
                    data = json.load(f)

                return data

            return None

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

        return data or {}


class ConfigOptions(BaseSettings):
    """Main configuration options for Fabricks framework."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    config: str = Field(
        validation_alias=AliasChoices("FABRICKS_CONFIG", "config"),
        default="none",
    )
    runtime: str = Field(
        validation_alias=AliasChoices("FABRICKS_RUNTIME", "runtime"),
        default="none",
    )
    notebooks: str = Field(
        validation_alias=AliasChoices("FABRICKS_NOTEBOOKS", "notebooks"),
        default="none",
    )
    job_config_from_yaml: bool = Field(
        validation_alias=AliasChoices("FABRICKS_IS_JOB_CONFIG_FROM_YAML", "job_config_from_yaml"),
        default=False,
    )
    debugmode: bool = Field(
        validation_alias=AliasChoices("FABRICKS_IS_DEBUGMODE", "debugmode"),
        default=False,
    )
    funmode: bool = Field(
        validation_alias=AliasChoices("FABRICKS_IS_FUNMODE", "funmode"),
        default=False,
    )
    devmode: bool = Field(
        validation_alias=AliasChoices("FABRICKS_IS_DEVMODE", "devmode"),
        default=False,
    )
    loglevel: int = Field(
        validation_alias=AliasChoices("FABRICKS_LOGLEVEL", "loglevel"),
        default=20,
    )

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
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ):
        # Order: env vars > hierarchical file > defaults
        return (
            init_settings,
            env_settings,
            HierarchicalFileSettingsSource(settings_cls),
            file_secret_settings,
        )
