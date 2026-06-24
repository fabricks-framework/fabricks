"""Common types and type aliases used across all models."""

from enum import Enum
from typing import Literal, Union

from pydantic import BaseModel, ConfigDict

from fabricks.models.config import config


class BronzeMode(str, Enum):
    MEMORY = "memory"
    APPEND = "append"
    REGISTER = "register"


class SilverMode(str, Enum):
    MEMORY = "memory"
    APPEND = "append"
    LATEST = "latest"
    UPDATE = "update"
    COMBINE = "combine"


class GoldMode(str, Enum):
    MEMORY = "memory"
    APPEND = "append"
    COMPLETE = "complete"
    UPDATE = "update"
    INVOKE = "invoke"
    REGISTER = "register"


# Backward-compatible aliases and union type
AllowedModesBronze = BronzeMode
AllowedModesSilver = SilverMode
AllowedModesGold = GoldMode
AllowedModes = Union[BronzeMode, SilverMode, GoldMode]

# File and operation types
AllowedFileFormats = Literal["json_array", "json", "jsonl", "csv", "parquet", "delta"]
AllowedOperations = Literal["upsert", "reload", "delete"]
AllowedTypes = Literal["manual", "default"]
AllowedOrigins = Literal["parser", "parent", "wait_for"]
AllowedFileFormatsRegister = Literal["delta", "parquet"]

# Constraint types
AllowedConstraintOptions = Literal["not enforced", "deferrable", "initially deferred", "norely", "rely"]
AllowedForeignKeyOptions = Literal["match full", "on update no action", "on delete no action"]

# Change Data Capture types
AllowedChangeDataCaptures = Literal["nocdc", "scd0", "scd1", "scd2"]


class SparkOptions(BaseModel):
    """Spark SQL and configuration options."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    sql: dict[str, str | bool | int] | None = None
    conf: dict[str, str | bool | int] | None = None


class BaseInvokerOptions(BaseModel):
    """Options for invoking notebooks during pre/post run operations."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    notebook: str | None = None
    timeout: int | None = None
    arguments: dict[str, str | bool | int] | None = None


class InvokerOptions(BaseModel):
    """Grouped invoker operations for pre/run/post execution."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    pre_run: list[BaseInvokerOptions] | None = None
    run: list[BaseInvokerOptions] | None = None
    post_run: list[BaseInvokerOptions] | None = None


class ExtenderOptions(BaseModel):
    """Configuration for runtime extenders."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    extender: str
    arguments: dict[str, str] | None = None


class UpdaterOptions(BaseModel):
    """Configuration for runtime updaters."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    columns: dict[str, str] | None = None


class RegisterOptions(BaseModel):
    """Options for registering tables."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    uri: str | None = None
    file_format: AllowedFileFormatsRegister | None = None


class DatabasePathOptions(BaseModel):
    """Path configuration for databases."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    storage: str


class Database(BaseModel):
    """Database configuration."""

    model_config = ConfigDict(extra=config.extra_config, frozen=True)

    name: str
    path_options: DatabasePathOptions
