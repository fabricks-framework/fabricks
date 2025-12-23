"""Common types and type aliases used across all models."""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict

# Step type definitions
TBronze = Literal["bronze"]
TSilver = Literal["silver"]
TGold = Literal["gold"]
TStep = Literal[TBronze, TSilver, TGold]

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

    model_config = ConfigDict(extra="forbid", frozen=True)

    sql: dict[str, str]
    conf: dict[str, str]


class InvokeOptions(BaseModel):
    """Options for invoking notebooks during pre/post run operations."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    notebook: str
    arguments: Optional[dict[str, str]] = None


class ExtenderOptions(BaseModel):
    """Configuration for runtime extenders."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    extender: str
    arguments: Optional[dict[str, str]] = None


class DatabasePathOptions(BaseModel):
    """Path configuration for databases."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage: str


class Database(BaseModel):
    """Database configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    path_options: DatabasePathOptions
