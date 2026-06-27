"""Fabricks models module - All Pydantic models for jobs, steps, and runtime configuration."""

# Common types and aliases
# CDC
from fabricks.models.cdc import CdcContext, QueryContext
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
    InvokerOptions,
    RegisterOptions,
    SparkOptions,
    UpdaterOptions,
)
from fabricks.models.dependency import JobDependency, SchemaDependencies

# Job models
from fabricks.models.job import BronzeOptions as JobBronzeOptions
from fabricks.models.job import (
    CheckOptions,
    JobConf,
    JobConfBase,
    JobConfBronze,
    JobConfGold,
    JobConfSilver,
    ParserOptions,
    TOptions,
)
from fabricks.models.job import GoldOptions as JobGoldOptions
from fabricks.models.job import SilverOptions as JobSilverOptions
from fabricks.models.path import Paths

# Runtime models
from fabricks.models.runtime import RuntimeConf, RuntimeOptions, RuntimePathOptions, RuntimeTimeoutOptions

# Schedule models
from fabricks.models.schedule import Schedule, ScheduleOptions

# Step models
from fabricks.models.step import BronzeConf as StepBronzeConf
from fabricks.models.step import BronzeOptions as StepBronzeOptions
from fabricks.models.step import GoldConf as StepGoldConf
from fabricks.models.step import GoldOptions as StepGoldOptions
from fabricks.models.step import PowerBI, Step, StepOptions, StepPathOptions, StepTimeoutOptions
from fabricks.models.step import SilverConf as StepSilverConf
from fabricks.models.step import SilverOptions as StepSilverOptions

# Table models
from fabricks.models.table import (
    ForeignKey,
    ForeignKeyOptions,
    PrimaryKey,
    PrimaryKeyOptions,
    StepTableOptions,
    TableOptions,
)

# Utility functions
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
    "SparkOptions",
    "UpdaterOptions",
    "RegisterOptions",
    # Job models
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
    "Paths",
    "SchemaDependencies",
    "TOptions",
    # Runtime models
    "RuntimeConf",
    "RuntimeOptions",
    "RuntimePathOptions",
    "RuntimeTimeoutOptions",
    # Step models
    "PowerBI",
    "Step",
    "StepBronzeConf",
    "StepBronzeOptions",
    "StepGoldConf",
    "StepGoldOptions",
    "StepOptions",
    "StepPathOptions",
    "StepSilverConf",
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
    # Schedule models
    "ScheduleOptions",
    "Schedule",
    # Utility functions
    "get_dependency_id",
    "get_job_id",
    # CDC
    "CdcContext",
    "QueryContext",
]
