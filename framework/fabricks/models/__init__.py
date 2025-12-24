"""Fabricks models module - All Pydantic models for jobs, steps, and runtime configuration."""

# Common types and aliases
from fabricks.models.common import (
    AllowedChangeDataCaptures,
    AllowedConstraintOptions,
    AllowedFileFormats,
    AllowedForeignKeyOptions,
    AllowedModes,
    AllowedModesBronze,
    AllowedModesGold,
    AllowedModesSilver,
    AllowedOperations,
    AllowedOrigins,
    AllowedTypes,
    Database,
    DatabasePathOptions,
    ExtenderOptions,
    InvokeOptions,
    SparkOptions,
    TBronze,
    TGold,
    TSilver,
    TStep,
)
from fabricks.models.dependency import JobDependency, SchemaDependencies

# Job models
from fabricks.models.job import BaseInvokerOptions
from fabricks.models.job import BronzeOptions as JobBronzeOptions
from fabricks.models.job import CheckOptions
from fabricks.models.job import GoldOptions as JobGoldOptions
from fabricks.models.job import (
    InvokerOptions,
    JobConf,
    JobConfBase,
    JobConfBronze,
    JobConfGold,
    JobConfSilver,
    ParserOptions,
)
from fabricks.models.job import SilverOptions as JobSilverOptions
from fabricks.models.job import TOptions
from fabricks.models.path import Path, Paths

# Runtime models
from fabricks.models.runtime import RuntimeConf, RuntimeOptions, RuntimePathOptions, RuntimeTimeoutOptions

# Step models
from fabricks.models.step import BronzeConf
from fabricks.models.step import BronzeOptions as StepBronzeOptions
from fabricks.models.step import GoldConf
from fabricks.models.step import GoldOptions as StepGoldOptions
from fabricks.models.step import PowerBI, SilverConf
from fabricks.models.step import SilverOptions as StepSilverOptions
from fabricks.models.step import Step, StepOptions, StepPathOptions, StepTimeoutOptions

# Table models
from fabricks.models.table import (
    ForeignKey,
    ForeignKeyOptions,
    PrimaryKey,
    PrimaryKeyOptions,
    StepTableOptions,
    TableOptions,
)
from fabricks.models.utils import get_dependency_id, get_job_id

__all__ = [
    # Common types
    "AllowedChangeDataCaptures",
    "AllowedConstraintOptions",
    "AllowedFileFormats",
    "AllowedForeignKeyOptions",
    "AllowedModes",
    "AllowedModesBronze",
    "AllowedModesGold",
    "AllowedModesSilver",
    "AllowedOperations",
    "AllowedOrigins",
    "AllowedTypes",
    "Database",
    "DatabasePathOptions",
    "ExtenderOptions",
    "InvokeOptions",
    "SparkOptions",
    "TBronze",
    "TGold",
    "TSilver",
    "TStep",
    # Job models
    "BaseInvokerOptions",
    "CheckOptions",
    "InvokerOptions",
    "JobBronzeOptions",
    "JobConf",
    "JobConfBase",
    "JobConfBronze",
    "JobConfGold",
    "JobConfSilver",
    "JobDependency",
    "JobGoldOptions",
    "JobSilverOptions",
    "Path",
    "Paths",
    "SchemaDependencies",
    "TOptions",
    # Runtime models
    "RuntimeConf",
    "RuntimeOptions",
    "RuntimePathOptions",
    "RuntimeTimeoutOptions",
    # Step models
    "BronzeConf",
    "GoldConf",
    "PowerBI",
    "SilverConf",
    "Step",
    "StepBronzeOptions",
    "StepGoldOptions",
    "StepOptions",
    "StepPathOptions",
    "StepSilverOptions",
    "StepTimeoutOptions",
    # Table models
    "ForeignKey",
    "ForeignKeyOptions",
    "PrimaryKey",
    "PrimaryKeyOptions",
    "StepTableOptions",
    "TableOptions",
    "ParserOptions",
    "get_dependency_id",
    "get_job_id",
]
