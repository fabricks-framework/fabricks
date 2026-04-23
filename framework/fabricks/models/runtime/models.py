"""Runtime configuration models."""

from datetime import timezone as tz
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fabricks.models.common import Database, ExtenderOptions, SparkOptions
from fabricks.models.config import ConfigOptions, config
from fabricks.models.runtime.utils import load_variables, perform_variable_substitution, resolve_runtime_paths
from fabricks.models.step import BronzeConf, GoldConf, PowerBI, SilverConf
from fabricks.utils.path import FileSharePath, GitPath


class RuntimePathOptions(BaseModel):
    """Path configuration for runtime components."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    storage: str
    udfs: str
    parsers: str
    schedules: str
    views: str
    requirements: str
    storage_credential: str | None = None
    extenders: str | None = None
    masks: str | None = None
    variables: str | None = None


class UDFOptions(BaseModel):
    prefix: str | None = None
    schema_name: str | None = Field(None, alias="schema")


class MaskOptions(BaseModel):
    prefix: str | None = None
    schema_name: str | None = Field(None, alias="schema")


class RuntimeResolvedPathOptions(BaseModel):
    """Resolved path objects for runtime components."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True, arbitrary_types_allowed=True)

    storage: FileSharePath
    udfs: GitPath
    parsers: GitPath
    schedules: GitPath
    views: GitPath
    requirements: GitPath
    extenders: GitPath
    masks: GitPath

    storages: dict[str, FileSharePath]
    runtimes: dict[str, GitPath]


class RuntimeTimeoutOptions(BaseModel):
    """Timeout settings for runtime operations."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    step: int = 3600
    job: int = 3600
    pre_run: int = 3600
    post_run: int = 3600


class RuntimeOptions(BaseModel):
    """Main runtime configuration options."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    secret_scope: str
    encryption_key: str | None = None
    unity_catalog: bool | None = None
    type_widening: bool | None = None
    catalog: str | None = None
    workers: int = 16
    timeouts: RuntimeTimeoutOptions
    retention_days: int = 7
    timezone: str = str(tz.utc)


class RuntimeConf(BaseModel):
    """Complete runtime configuration.

    Variables can be defined inline as a dict, or loaded from a file specified in
    path_options.variables (relative to runtime config directory or absolute path).
    """

    model_config = ConfigDict(extra=config.extra_config, frozen=True, arbitrary_types_allowed=True)

    name: str
    options: RuntimeOptions
    path_options: RuntimePathOptions
    extender_options: ExtenderOptions | None = None
    spark_options: SparkOptions | None = None
    udf_options: UDFOptions | None = None
    mask_options: MaskOptions | None = None
    bronze: list[BronzeConf] | None = None
    silver: list[SilverConf] | None = None
    gold: list[GoldConf] | None = None
    powerbi: list[PowerBI] | None = None
    databases: list[Database] | None = None
    variables: dict[str, str] | None = None
    credentials: dict[str, str] | None = None

    config: ClassVar[ConfigOptions] = config

    @model_validator(mode="before")
    @classmethod
    def _substitute_variables(cls, data: Any) -> Any:
        """Perform variable substitution during parsing.

        Loads variables from path_options.variables if defined, otherwise uses
        inline variables dict. Uses config.path_to_config to resolve relative paths.
        """
        if not isinstance(data, dict):
            return data

        # Get config path from ConfigOptions
        config_path = Path(cls.config.path_to_config)

        # Extract variables path from path_options if present
        variables_path = None
        if isinstance(data.get("path_options"), dict):
            variables_path = data["path_options"].get("variables")

        # Step 1: Load variables
        variables = load_variables(
            data=data,
            config_path=config_path,
            variables_path=variables_path,
        )

        # Step 2: Perform substitution
        return perform_variable_substitution(data=data, variables=variables)

    @cached_property
    def resolved_path_options(self) -> RuntimeResolvedPathOptions:
        """Get all runtime paths resolved as Path objects (cached)."""
        resolved = resolve_runtime_paths(
            path_options=self.path_options.model_dump(),
            variables=self.variables,
            bronze=self.bronze,
            silver=self.silver,
            gold=self.gold,
            databases=self.databases,
            base_runtime=self.config.resolved_paths.runtime,
        )

        return RuntimeResolvedPathOptions(**resolved)
