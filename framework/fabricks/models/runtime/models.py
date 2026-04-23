"""Runtime configuration models."""

from datetime import timezone as tz
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator

from fabricks.models.common import Database, ExtenderOptions, SparkOptions
from fabricks.models.config import ConfigOptions, config
from fabricks.models.runtime.utils import (
    _as_variables,
    _build_variable_lookup,
    _resolve_variables_path,
    _substitute_value,
)
from fabricks.models.step import BronzeConf, GoldConf, PowerBI, SilverConf
from fabricks.utils.path import FileSharePath, GitPath, resolve_fileshare_path, resolve_git_path


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
    """Complete runtime configuration."""

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
    def _substitute_variables(cls, data: Any, info) -> Any:
        """Perform variable substitution during parsing.

        Requires context with 'config_path' and optionally 'external_variables_file'.
        Example:
            RuntimeConf.model_validate(
                data,
                context={"config_path": Path(...), "external_variables_file": "..."}
            )
        """
        if not isinstance(data, dict):
            return data

        context = info.context or {}
        config_path = context.get("config_path")
        if not config_path:
            return data

        if isinstance(config_path, str):
            config_path = Path(config_path)

        external_variables_file = context.get("external_variables_file")

        prepared = dict(data)
        base_variables = _as_variables(prepared.get("variables"), source="runtime config")
        variables_path = _resolve_variables_path(
            conf_data=prepared,
            config_path=config_path,
            external_variables_file=external_variables_file,
        )

        file_variables: dict[str, Any] = {}
        if variables_path:
            try:
                import yaml

                with open(variables_path, encoding="utf-8") as f:
                    file_variables = _as_variables(yaml.safe_load(f), source=str(variables_path))
            except FileNotFoundError as exc:
                raise FileNotFoundError(
                    f"variables file '{variables_path}' referenced by config '{config_path}' was not found",
                ) from exc

        merged_variables = {**base_variables, **file_variables}
        prepared["variables"] = merged_variables
        prepared.pop("variables_file", None)

        if not merged_variables:
            return prepared

        return _substitute_value(prepared, _build_variable_lookup(merged_variables))

    @computed_field
    @property
    def resolved_path_options(self) -> RuntimeResolvedPathOptions:
        """Get all runtime paths resolved as Path objects."""
        return self._resolve_paths()

    def _resolve_paths(self) -> RuntimeResolvedPathOptions:
        """
        Get all runtime paths resolved as Path objects.

        Args:
            runtime: The base runtime path (e.g., PATH_RUNTIME)

        Returns:
            RuntimeResolvedPathOptions with all paths resolved
        """
        variables_as_strings = None
        if self.variables:
            variables_as_strings = {key: str(value) for key, value in self.variables.items()}

        # Collect all storage paths with variable substitution
        storage_paths: dict[str, FileSharePath] = {
            "fabricks": resolve_fileshare_path(
                self.path_options.storage,
                variables=variables_as_strings,
            ),
        }

        # Add storage paths for bronze/silver/gold/databases
        for objects in [self.bronze, self.silver, self.gold, self.databases]:
            if objects:
                for obj in objects:
                    storage_paths[obj.name] = resolve_fileshare_path(
                        obj.path_options.storage,
                        variables=variables_as_strings,
                    )

        root = self.config.resolved_paths.runtime

        # Collect all runtime paths with base path joining
        runtime_paths: dict[str, GitPath] = {}
        for objects in [self.bronze, self.silver, self.gold]:
            if objects:
                for obj in objects:
                    runtime_paths[obj.name] = resolve_git_path(
                        obj.path_options.runtime,
                        base=root,
                    )

        return RuntimeResolvedPathOptions(
            storage=storage_paths["fabricks"],
            udfs=resolve_git_path(
                path=self.path_options.udfs,
                base=root,
            ),
            parsers=resolve_git_path(
                path=self.path_options.parsers,
                base=root,
            ),
            schedules=resolve_git_path(
                path=self.path_options.schedules,
                base=root,
            ),
            views=resolve_git_path(
                path=self.path_options.views,
                base=root,
            ),
            requirements=resolve_git_path(
                path=self.path_options.requirements,
                base=root,
            ),
            extenders=resolve_git_path(
                path=self.path_options.extenders,
                base=root,
                default="fabricks/extenders",
            ),
            masks=resolve_git_path(
                path=self.path_options.masks,
                base=root,
                default="fabricks/masks",
            ),
            storages=storage_paths,
            runtimes=runtime_paths,
        )
