"""Step configuration models."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict

from fabricks.models.common import InvokerOptions
from fabricks.models.table import StepTableOptions


class StepTimeoutOptions(BaseModel):
    """Optional timeout overrides for individual steps."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step: Optional[int] = None
    job: Optional[int] = None
    pre_run: Optional[int] = None
    post_run: Optional[int] = None


class StepPathOptions(BaseModel):
    """Path configuration for steps."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    runtime: str
    storage: str


class StepOptions(BaseModel):
    """Base step configuration options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order: int
    workers: Optional[int] = None
    timeouts: Optional[StepTimeoutOptions] = None
    extenders: Optional[List[str]] = None
    invokers: Optional[InvokerOptions] = None


class BronzeOptions(StepOptions):
    """Bronze layer step options."""


class SilverOptions(StepOptions):
    """Silver layer step options."""

    parent: str
    stream: Optional[bool] = None
    local_checkpoint: Optional[bool] = None


class GoldOptions(StepOptions):
    """Gold layer step options."""

    schema_drift: Optional[bool] = None
    metadata: Optional[bool] = None


class Step(BaseModel):
    """Base step configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str

    timeouts: Optional[StepTimeoutOptions] = None
    extenders: Optional[List[str]] = None
    pre_run: Optional[InvokerOptions] = None
    post_run: Optional[InvokerOptions] = None
    path_options: StepPathOptions
    table_options: Optional[StepTableOptions] = None


class BronzeConf(Step):
    """Bronze layer step configuration."""

    options: BronzeOptions


class SilverConf(Step):
    """Silver layer step configuration."""

    options: SilverOptions


class GoldConf(Step):
    """Gold layer step configuration."""

    options: GoldOptions


class PowerBI(Step):
    """PowerBI configuration."""
