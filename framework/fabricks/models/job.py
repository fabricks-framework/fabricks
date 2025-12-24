"""Job configuration models."""

from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, computed_field

from fabricks.models.common import (
    AllowedChangeDataCaptures,
    AllowedModesBronze,
    AllowedModesGold,
    AllowedModesSilver,
    AllowedOperations,
    AllowedTypes,
    ExtenderOptions,
    SparkOptions,
    TBronze,
    TGold,
    TSilver,
    TStep,
)
from fabricks.models.table import TableOptions
from fabricks.models.utils import get_job_id


class BaseInvokerOptions(BaseModel):
    """Base configuration for notebook invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    notebook: str
    timeout: int
    arguments: Optional[dict[str, str]] = None


class InvokerOptions(BaseModel):
    """Grouped invoker operations for pre/run/post execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pre_run: Optional[List[BaseInvokerOptions]] = None
    run: Optional[List[BaseInvokerOptions]] = None
    post_run: Optional[List[BaseInvokerOptions]] = None


class CheckOptions(BaseModel):
    """Data quality check options for jobs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    skip: Optional[bool] = None
    pre_run: Optional[bool] = None
    post_run: Optional[bool] = None
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    count_must_equal: Optional[str] = None


class ParserOptions(BaseModel):
    file_format: Optional[str] = None
    read_options: Optional[dict[str, str]] = None


class BronzeOptions(BaseModel):
    """Bronze layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesBronze
    change_data_capture: AllowedChangeDataCaptures = "none"

    uri: str
    parser: str
    source: str
    type: Optional[AllowedTypes] = None
    keys: Optional[List[str]] = None
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    encrypted_columns: Optional[List[str]] = None
    calculated_columns: Optional[dict[str, str]] = None
    operation: Optional[AllowedOperations] = None
    timeout: Optional[int] = None


class SilverOptions(BaseModel):
    """Silver layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesSilver
    change_data_capture: AllowedChangeDataCaptures
    type: Optional[AllowedTypes] = None
    parents: Optional[List[str]] = None
    filter_where: Optional[str] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    deduplicate: Optional[bool] = None
    stream: Optional[bool] = None
    order_duplicate_by: Optional[dict[str, str]] = None
    timeout: Optional[int] = None


class GoldOptions(BaseModel):
    """Gold layer job options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: AllowedModesGold
    change_data_capture: AllowedChangeDataCaptures
    type: Optional[AllowedTypes] = None
    update_where: Optional[str] = None
    parents: Optional[List[str]] = None
    optimize: Optional[bool] = None
    compute_statistics: Optional[bool] = None
    vacuum: Optional[bool] = None
    no_drop: Optional[bool] = None
    deduplicate: Optional[bool] = None
    rectify_as_upserts: Optional[bool] = None
    correct_valid_from: Optional[bool] = None
    persist_last_timestamp: Optional[bool] = None
    persist_last_updated_timestamp: Optional[bool] = None
    table: Optional[str] = None
    notebook: Optional[bool] = None
    requirements: Optional[bool] = None
    timeout: Optional[int] = None
    metadata: Optional[bool] = None
    last_updated: Optional[bool] = None


TOptions = Union[BronzeOptions, SilverOptions, GoldOptions]


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
    table_options: Optional[TableOptions] = None
    check_options: Optional[CheckOptions] = None
    spark_options: Optional[SparkOptions] = None
    invoker_options: Optional[InvokerOptions] = None
    extender_options: Optional[List[ExtenderOptions]] = None
    tags: Optional[List[str]] = None
    comment: Optional[str] = None


class JobConfBronze(JobConfBase):
    """Bronze-specific job configuration."""

    step: TBronze
    options: BronzeOptions
    parser_options: Optional["ParserOptions"] = None


class JobConfSilver(JobConfBase):
    """Silver-specific job configuration."""

    step: TSilver
    options: SilverOptions


class JobConfGold(JobConfBase):
    """Gold-specific job configuration."""

    step: TGold
    options: GoldOptions


JobConf = Union[JobConfBronze, JobConfSilver, JobConfGold]
