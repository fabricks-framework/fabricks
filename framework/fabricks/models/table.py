"""Table-related options and constraint models."""

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict

from fabricks.models.common import AllowedConstraintOptions, AllowedForeignKeyOptions


class ForeignKeyOptions(BaseModel):
    """Options for foreign key constraints."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    foreign_key: Optional[AllowedForeignKeyOptions] = None
    constraint: Optional[AllowedConstraintOptions] = None


class PrimaryKeyOptions(BaseModel):
    """Options for primary key constraints."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    constraint: Optional[AllowedConstraintOptions] = None


class ForeignKey(BaseModel):
    """Foreign key constraint definition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    keys: List[str]
    reference: str
    options: Optional[ForeignKeyOptions] = None


class PrimaryKey(BaseModel):
    """Primary key constraint definition."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    keys: List[str]
    options: Optional[PrimaryKeyOptions] = None


class TableOptions(BaseModel):
    """Comprehensive table configuration options for jobs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    cluster_by: Optional[List[str]] = None
    powerbi: Optional[bool] = None
    maximum_compatibility: Optional[bool] = None
    bloomfilter_by: Optional[List[str]] = None
    constraints: Optional[dict[Any, Any]] = None
    properties: Optional[dict[Any, Any]] = None
    comment: Optional[str] = None
    calculated_columns: Optional[dict[Any, Any]] = None
    masks: Optional[dict[Any, Any]] = None
    comments: Optional[dict[Any, Any]] = None
    retention_days: Optional[int] = None
    primary_key: Optional[dict[str, PrimaryKey]] = None
    foreign_keys: Optional[dict[str, ForeignKey]] = None


class StepTableOptions(BaseModel):
    """Simplified table options for step-level configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    powerbi: Optional[bool] = None
    liquid_clustering: Optional[bool] = None
    properties: Optional[dict[Any, Any]] = None
    retention_days: Optional[int] = None
    masks: Optional[dict[Any, Any]] = None
