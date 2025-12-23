"""Runtime configuration models."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from fabricks.models.common import Database, ExtenderOptions, SparkOptions
from fabricks.models.step import BronzeConf, GoldConf, PowerBI, SilverConf


class RuntimePathOptions(BaseModel):
    """Path configuration for runtime components."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage: str
    udfs: str
    parsers: str
    schedules: str
    views: str
    requirements: str
    storage_credential: Optional[str] = None
    extenders: Optional[str] = None
    masks: Optional[str] = None


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
    unity_catalog: Optional[bool] = None
    type_widening: Optional[bool] = None
    catalog: Optional[str] = None
    workers: int
    timeouts: RuntimeTimeoutOptions
    retention_days: int
    timezone: Optional[str] = None


class RuntimeConf(BaseModel):
    """Complete runtime configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    options: RuntimeOptions
    path_options: RuntimePathOptions
    extender_options: Optional[ExtenderOptions] = None
    spark_options: SparkOptions
    bronze: Optional[List[BronzeConf]] = None
    silver: Optional[List[SilverConf]] = None
    gold: Optional[List[GoldConf]] = None
    powerbi: Optional[List[PowerBI]] = None
    databases: Optional[List[Database]] = None
    variables: Optional[dict[str, str]] = None
    credentials: Optional[List[dict[str, str]]] = None
