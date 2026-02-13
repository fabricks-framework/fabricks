"""Runtime configuration models."""

from datetime import timezone as tz
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, computed_field

from fabricks.models.common import Database, ExtenderOptions, SparkOptions
from fabricks.models.config import ConfigOptions
from fabricks.models.step import BronzeConf, GoldConf, PowerBI, SilverConf
from fabricks.utils.path import FileSharePath, GitPath, resolve_fileshare_path, resolve_git_path


class RuntimePathOptions(BaseModel):
    """Path configuration for runtime components."""

    model_config = ConfigDict(extra="ignore", frozen=True)

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

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

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

    model_config = ConfigDict(extra="ignore", frozen=True)

    step: int = 3600
    job: int = 3600
    pre_run: int = 3600
    post_run: int = 3600


class RuntimeOptions(BaseModel):
    """Main runtime configuration options."""

    model_config = ConfigDict(extra="ignore", frozen=True)

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

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

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

    config: ClassVar[ConfigOptions] = ConfigOptions()

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
        # Collect all storage paths with variable substitution
        storage_paths: dict[str, FileSharePath] = {
            "fabricks": resolve_fileshare_path(self.path_options.storage, variables=self.variables),
        }

        # Add storage paths for bronze/silver/gold/databases
        for objects in [self.bronze, self.silver, self.gold, self.databases]:
            if objects:
                for obj in objects:
                    storage_paths[obj.name] = resolve_fileshare_path(
                        obj.path_options.storage,
                        variables=self.variables,
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
