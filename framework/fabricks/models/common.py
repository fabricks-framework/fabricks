"""Common types and type aliases used across all models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

# Mode type definitions
AllowedModesBronze = Literal["memory", "append", "register"]
AllowedModesSilver = Literal["memory", "append", "latest", "update", "combine"]
AllowedModesGold = Literal["memory", "append", "complete", "update", "invoke"]
AllowedModes = Literal[AllowedModesBronze, AllowedModesSilver, AllowedModesGold]

# File and operation types
AllowedFileFormats = Literal["json_array", "json", "jsonl", "csv", "parquet", "delta"]
AllowedOperations = Literal["upsert", "reload", "delete"]
AllowedTypes = Literal["manual", "default"]
AllowedOrigins = Literal["parser", "job"]

# Constraint types
AllowedConstraintOptions = Literal["not enforced", "deferrable", "initially deferred", "norely", "rely"]
AllowedForeignKeyOptions = Literal["match full", "on update no action", "on delete no action"]

# Change Data Capture types
AllowedChangeDataCaptures = Literal["nocdc", "scd1", "scd2", "none"]


class SparkOptions(BaseModel):
    """Spark SQL and configuration options."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    sql: dict[str, str | bool | int] | None = None
    conf: dict[str, str | bool | int] | None = None


class BaseInvokerOptions(BaseModel):
    """Options for invoking notebooks during pre/post run operations."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    notebook: str | None = None
    timeout: int | None = None
    arguments: dict[str, str | bool | int] | None = None


class InvokerOptions(BaseModel):
    """Grouped invoker operations for pre/run/post execution."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    pre_run: list[BaseInvokerOptions] | None = None
    run: list[BaseInvokerOptions] | None = None
    post_run: list[BaseInvokerOptions] | None = None


class ExtenderOptions(BaseModel):
    """Configuration for runtime extenders."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    extender: str
    arguments: dict[str, str] | None = None


class DatabasePathOptions(BaseModel):
    """Path configuration for databases."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    storage: str


class Database(BaseModel):
    """Database configuration."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    name: str
    path_options: DatabasePathOptions
