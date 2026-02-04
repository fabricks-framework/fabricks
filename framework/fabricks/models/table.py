"""Table-related options and constraint models."""

from pydantic import BaseModel, ConfigDict

from fabricks.models.common import AllowedConstraintOptions, AllowedForeignKeyOptions


class ForeignKeyOptions(BaseModel):
    """Options for foreign key constraints."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    foreign_key: AllowedForeignKeyOptions | None = None
    constraint: AllowedConstraintOptions | None = None


class PrimaryKeyOptions(BaseModel):
    """Options for primary key constraints."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    constraint: AllowedConstraintOptions | None = None


class ForeignKey(BaseModel):
    """Foreign key constraint definition."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    keys: list[str]
    reference: str
    options: ForeignKeyOptions | None = None


class PrimaryKey(BaseModel):
    """Primary key constraint definition."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    keys: list[str]
    options: PrimaryKeyOptions | None = None


class TableOptions(BaseModel):
    """Comprehensive table configuration options for jobs."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    identity: bool | None = None
    liquid_clustering: bool | None = None
    partition_by: list[str] | None = None
    zorder_by: list[str] | None = None
    cluster_by: list[str] | None = None
    powerbi: bool | None = None
    maximum_compatibility: bool | None = None
    bloomfilter_by: list[str] | None = None
    constraints: dict[str, str | bool | int] | None = None
    properties: dict[str, str | bool | int] | None = None
    comment: str | None = None
    calculated_columns: dict[str, str | bool | int] | None = None
    masks: dict[str, str] | None = None
    comments: dict[str, str | bool | int] | None = None
    retention_days: int | None = None
    primary_key: dict[str, PrimaryKey] | None = None
    foreign_keys: dict[str, ForeignKey] | None = None


class StepTableOptions(BaseModel):
    """Simplified table options for step-level configuration."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    powerbi: bool | None = None
    liquid_clustering: bool | None = None
    properties: dict[str, str | bool | int] | None = None
    retention_days: int | None = None
    masks: dict[str, str] | None = None
