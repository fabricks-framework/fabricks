"""Runtime configuration models."""

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
    storage_credential: str | None = None
    extenders: str | None = None
    masks: str | None = None


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
