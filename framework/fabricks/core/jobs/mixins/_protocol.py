from __future__ import annotations

from typing import Optional, Protocol, Sequence, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc import BaseCDC
from fabricks.core.jobs.config import JobConfig
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.models import (
    AllowedModes,
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
    Paths,
    RuntimeOptions,
    SparkOptions,
    StepBronzeConf,
    StepGoldConf,
    StepOptions,
    StepSilverConf,
    StepTableOptions,
    TableOptions,
    TOptions,
    UpdaterOptions,
)
from fabricks.models.cdc import CdcContext


class JobProtocol(Protocol):
    step: str
    topic: str
    item: str
    job_id: str
    spark: SparkSession
    table: Table
    config: JobConfig
    _spark: Optional[SparkSession] = None  # Keep mutable - has side effects
    _udf_registered: Optional[bool] = None  # Keep mutable - state flag

    # ConfiguratorMixin-provided properties
    @property
    def qualified_name(self) -> str: ...

    @property
    def paths(self) -> Paths: ...

    @property
    def mode(self) -> AllowedModes: ...

    @property
    def step_options(self) -> StepOptions: ...

    @property
    def change_data_capture(self) -> str: ...

    @property
    def check_options(self) -> Optional[CheckOptions]: ...

    @property
    def invoker_options(self) -> Optional[InvokerOptions]: ...

    @property
    def extender_options(self) -> Optional[list[ExtenderOptions]]: ...

    @property
    def updater_options(self) -> Optional[UpdaterOptions]: ...

    @property
    def spark_options(self) -> Optional[SparkOptions]: ...

    @property
    def table_options(self) -> Optional[TableOptions]: ...

    @property
    def step_table_options(self) -> Optional[StepTableOptions]: ...

    @property
    def runtime_options(self) -> RuntimeOptions: ...

    @property
    def step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]: ...

    @property
    def timeout(self) -> int: ...

    @property
    def cdc(self) -> BaseCDC: ...  # NoCDC | SCD0 | SCD1 | SCD2 — Any avoids circular import

    def maintain(
        self,
        vacuum: Optional[bool] = None,
        optimize: Optional[bool] = None,
        compute_statistics: Optional[bool] = None,
    ) -> None: ...

    def register_udfs(self) -> None: ...

    # JobABC abstract interface
    @property
    def stream(self) -> bool: ...

    @property
    def schema_drift(self) -> bool: ...

    @property
    def persist(self) -> bool: ...

    @property
    def virtual(self) -> bool: ...

    @property
    def options(self) -> TOptions: ...

    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    def base_transform(self, df: DataFrame) -> DataFrame: ...

    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> CdcContext: ...

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs) -> None: ...

    # CheckerMixin methods (used by ProcessorMixin)
    def check_pre_run(self) -> None: ...

    def check_post_run(self) -> None: ...

    def check_post_run_extra(self) -> None: ...

    def check_skip_run(self) -> None: ...

    def check_run_before(self) -> None: ...

    def check_run_after(self) -> None: ...

    # InvokerMixin methods (used by ProcessorMixin)
    def invoke_pre_run(self, schedule: Optional[str] = None) -> None: ...

    def invoke_post_run(self, schedule: Optional[str] = None) -> None: ...

    # GeneratorMixin methods (used by ProcessorMixin)
    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]: ...

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False) -> None: ...

    def create_or_replace_view(self) -> None: ...

    def restore(self, last_version: Optional[str] = None, last_batch: Optional[str] = None) -> None: ...
