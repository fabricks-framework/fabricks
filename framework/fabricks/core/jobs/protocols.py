from __future__ import annotations

from typing import List, Optional, Protocol, Sequence, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc import CDCIntentContext
from fabricks.cdc.base import BaseCDC
from fabricks.metastore.table import Table
from fabricks.models import (
    AllowedChangeDataCaptures,
    AllowedModes,
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
    JobDependency,
    Paths,
    RuntimeOptions,
    SparkOptions,
    StepBronzeConf,
    StepGoldConf,
    StepSilverConf,
    StepTableOptions,
    TableOptions,
    TOptions,
)


class JobProtocol(Protocol):
    # identity
    @property
    def step(self) -> str: ...

    @property
    def topic(self) -> str: ...

    @property
    def item(self) -> str: ...

    @property
    def job_id(self) -> str: ...

    @property
    def qualified_name(self) -> str: ...

    @property
    def timeout(self) -> int: ...

    # spark
    @property
    def spark(self) -> SparkSession: ...

    # table & CDC
    @property
    def table(self) -> Table: ...

    @property
    def cdc(self) -> BaseCDC: ...

    @property
    def change_data_capture(self) -> AllowedChangeDataCaptures: ...

    @property
    def mode(self) -> AllowedModes: ...

    @property
    def stream(self) -> bool: ...

    @property
    def persist(self) -> bool: ...

    @property
    def virtual(self) -> bool: ...

    # paths & options
    @property
    def paths(self) -> Paths: ...

    @property
    def options(self) -> TOptions: ...

    @property
    def step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]: ...

    @property
    def runtime_options(self) -> RuntimeOptions: ...

    @property
    def table_options(self) -> Optional[TableOptions]: ...

    @property
    def step_table_options(self) -> Optional[StepTableOptions]: ...

    @property
    def spark_options(self) -> Optional[SparkOptions]: ...

    @property
    def check_options(self) -> Optional[CheckOptions]: ...

    @property
    def invoker_options(self) -> Optional[InvokerOptions]: ...

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]: ...

    # methods
    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> CDCIntentContext: ...

    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    def base_transform(self, df: DataFrame) -> DataFrame: ...

    def get_dependencies(self) -> Sequence[JobDependency]: ...
