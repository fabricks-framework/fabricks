"""Job configuration models."""

from pydantic import BaseModel, ConfigDict, computed_field

from fabricks.models.common import (
    AllowedChangeDataCaptures,
    AllowedModesBronze,
    AllowedModesGold,
    AllowedModesSilver,
    AllowedOperations,
    AllowedTypes,
    ExtenderOptions,
    InvokerOptions,
    SparkOptions,
    TBronze,
    TGold,
    TSilver,
    TStep,
)
from fabricks.models.table import TableOptions
from fabricks.models.utils import get_job_id


class CheckOptions(BaseModel):
    """Data quality check options for jobs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    skip: bool | None = None
    pre_run: bool | None = None
    post_run: bool | None = None
    min_rows: int | None = None
    max_rows: int | None = None
    count_must_equal: str | None = None


class ParserOptions(BaseModel):
    file_format: str | None = None
    read_options: dict[str, str] | None = None


class BronzeOptions(BaseModel):
    """Bronze layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesBronze
    change_data_capture: AllowedChangeDataCaptures = "none"

    uri: str
    parser: str
    source: str
    type: AllowedTypes | None = None
    keys: list[str] | None = None
    parents: list[str] | None = None
    filter_where: str | None = None
    optimize: bool | None = None
    compute_statistics: bool | None = None
    vacuum: bool | None = None
    no_drop: bool | None = None
    encrypted_columns: list[str] | None = None
    calculated_columns: dict[str, str] | None = None
    operation: AllowedOperations | None = None
    timeout: int | None = None


class SilverOptions(BaseModel):
    """Silver layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesSilver
    change_data_capture: AllowedChangeDataCaptures
    type: AllowedTypes | None = None
    parents: list[str] | None = None
    filter_where: str | None = None
    optimize: bool | None = None
    compute_statistics: bool | None = None
    vacuum: bool | None = None
    no_drop: bool | None = None
    deduplicate: bool | None = None
    stream: bool | None = None
    order_duplicate_by: dict[str, str] | None = None
    timeout: int | None = None


class GoldOptions(BaseModel):
    """Gold layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesGold
    change_data_capture: AllowedChangeDataCaptures
    type: AllowedTypes | None = None
    update_where: str | None = None
    parents: list[str] | None = None
    optimize: bool | None = None
    compute_statistics: bool | None = None
    vacuum: bool | None = None
    no_drop: bool | None = None
    deduplicate: bool | None = None
    rectify_as_upserts: bool | None = None
    correct_valid_from: bool | None = None
    persist_last_timestamp: bool | None = None
    persist_last_updated_timestamp: bool | None = None
    table: str | None = None
    notebook: bool | None = None
    requirements: bool | None = None
    timeout: int | None = None
    metadata: bool | None = None
    last_updated: bool | None = None


TOptions = BronzeOptions | SilverOptions | GoldOptions


class JobConfBase(BaseModel):
    """Base job configuration with computed fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step: TStep
    topic: str
    item: str

    @computed_field  # type: ignore[misc]
    @property
    def job_id(self) -> str:
        """Computed job_id from step, topic, and item."""
        return get_job_id(step=self.step, topic=self.topic, item=self.item)

    options: TOptions
    table_options: TableOptions | None = None
    check_options: CheckOptions | None = None
    spark_options: SparkOptions | None = None
    invoker_options: InvokerOptions | None = None
    extender_options: list[ExtenderOptions] | None = None
    tags: list[str] | None = None
    comment: str | None = None


class JobConfBronze(JobConfBase):
    """Bronze-specific job configuration."""

    step: TBronze
    options: BronzeOptions
    parser_options: ParserOptions | None = None


class JobConfSilver(JobConfBase):
    """Silver-specific job configuration."""

    step: TSilver
    options: SilverOptions


class JobConfGold(JobConfBase):
    """Gold-specific job configuration."""

    step: TGold
    options: GoldOptions


JobConf = JobConfBronze | JobConfSilver | JobConfGold
