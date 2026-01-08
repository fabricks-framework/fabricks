"""Step configuration models."""

from pydantic import BaseModel, ConfigDict

from fabricks.models.common import BaseInvokerOptions, SparkOptions
from fabricks.models.table import StepTableOptions


class StepInvokerOptions(BaseModel):
    """Grouped invoker operations for pre/run/post execution."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pre_run: list[BaseInvokerOptions] | None = None
    post_run: list[BaseInvokerOptions] | None = None


class StepTimeoutOptions(BaseModel):
    """Optional timeout overrides for individual steps."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step: int | None = None
    job: int | None = None
    pre_run: int | None = None
    post_run: int | None = None


class StepPathOptions(BaseModel):
    """Path configuration for steps."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    runtime: str
    storage: str


class StepOptions(BaseModel):
    """Base step configuration options."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    order: int
    workers: int | None = None
    timeouts: StepTimeoutOptions | None = None


class BronzeOptions(StepOptions):
    """Bronze layer step options."""


class SilverOptions(StepOptions):
    """Silver layer step options."""

    parent: str
    stream: bool | None = None
    local_checkpoint: bool | None = None


class GoldOptions(StepOptions):
    """Gold layer step options."""

    schema_drift: bool | None = None
    metadata: bool | None = None


class Step(BaseModel):
    """Base step configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    path_options: StepPathOptions
    table_options: StepTableOptions | None = None
    extender_options: list[str] | None = None
    invoker_options: StepInvokerOptions | None = None
    spark_options: SparkOptions | None = None


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

    path_options: StepPathOptions | None = None
