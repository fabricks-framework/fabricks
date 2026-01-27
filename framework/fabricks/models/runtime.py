"""Runtime configuration models."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, computed_field

from fabricks.models.common import Database, ExtenderOptions, SparkOptions
from fabricks.models.step import BronzeConf, GoldConf, PowerBI, SilverConf
from fabricks.utils.path import Path, resolve_path


class RuntimePathOptions(BaseModel):
    """Path configuration for runtime components."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage: str
    udfs: str
    parsers: str
    schedules: str
    views: str
    requirements: str
    storage_credential: str | None = None
    extenders: str | None = None
    masks: str | None = None


class RuntimeResolvedPathOptions(BaseModel):
    """Resolved path objects for runtime components."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    storage: Path
    udfs: Path
    parsers: Path
    schedules: Path
    views: Path
    requirements: Path
    extenders: Path
    masks: Path

    storage_paths: dict[str, Path]
    runtime_paths: dict[str, Path]


class RuntimeTimeoutOptions(BaseModel):
    """Timeout settings for runtime operations."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step: int
    job: int
    pre_run: int
    post_run: int


class RuntimeOptions(BaseModel):
    """Main runtime configuration options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    secret_scope: str
    encryption_key: str | None = None
    unity_catalog: bool | None = None
    type_widening: bool | None = None
    catalog: str | None = None
    workers: int
    timeouts: RuntimeTimeoutOptions
    retention_days: int
    timezone: str | None = None


class RuntimeConf(BaseModel):
    """Complete runtime configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    options: RuntimeOptions
    path_options: RuntimePathOptions
    extender_options: ExtenderOptions | None = None
    spark_options: SparkOptions
    bronze: list[BronzeConf] | None = None
    silver: list[SilverConf] | None = None
    gold: list[GoldConf] | None = None
    powerbi: list[PowerBI] | None = None
    databases: list[Database] | None = None
    variables: dict[str, str] | None = None
    credentials: list[dict[str, str]] | None = None

    @property
    @computed_field
    def resolved_path_options(self) -> RuntimeResolvedPathOptions:
        """Get all runtime paths resolved as Path objects."""
        return self._resolve_paths(runtime=Path.from_uri("fabricks/runtime"))

    def _resolve_path(
        self,
        path: Optional[str],
        default: Optional[str] = None,
        base: Optional[Path] = None,
        apply_variables: bool = False,
    ) -> Path:
        return resolve_path(
            path=path,
            default=default,
            base=base,
            apply_variables=apply_variables,
            variables=self.variables or {},
        )

    def _resolve_paths(self, runtime: Path) -> RuntimeResolvedPathOptions:
        """
        Get all runtime paths resolved as Path objects.

        Args:
            runtime: The base runtime path (e.g., PATH_RUNTIME)

        Returns:
            RuntimeResolvedPathOptions with all paths resolved
        """
        # Collect all storage paths with variable substitution
        storage_paths: dict[str, Path] = {
            "fabricks": self._resolve_path(self.path_options.storage, apply_variables=True)
        }

        # Add storage paths for bronze/silver/gold/databases
        for objects in [self.bronze, self.silver, self.gold, self.databases]:
            if objects:
                for obj in objects:
                    storage_paths[obj.name] = resolve_path(
                        obj.path_options.storage, apply_variables=True, variables=self.variables or {}
                    )

        # Collect all runtime paths with base path joining
        runtime_paths: dict[str, Path] = {}
        for objects in [self.bronze, self.silver, self.gold]:
            if objects:
                for obj in objects:
                    runtime_paths[obj.name] = resolve_path(obj.path_options.runtime, base=runtime)

        return RuntimeResolvedPathOptions(
            storage=storage_paths["fabricks"],
            udfs=self._resolve_path(self.path_options.udfs, base=runtime),
            parsers=self._resolve_path(self.path_options.parsers, base=runtime),
            schedules=self._resolve_path(self.path_options.schedules, base=runtime),
            views=self._resolve_path(self.path_options.views, base=runtime),
            requirements=self._resolve_path(self.path_options.requirements, base=runtime),
            extenders=self._resolve_path(self.path_options.extenders, default="fabricks/extenders", base=runtime),
            masks=self._resolve_path(self.path_options.masks, default="fabricks/masks", base=runtime),
            storage_paths=storage_paths,
            runtime_paths=runtime_paths,
        )
