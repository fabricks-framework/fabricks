from __future__ import annotations

from typing import Any, List, Optional, Protocol, Sequence, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc import CDCIntentContext
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.models import (
    AllowedChangeDataCaptures,
    AllowedModes,
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
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


class CheckableJob(Protocol):
    """Narrow interface required by JobChecker."""

    @property
    def check_options(self) -> Optional[CheckOptions]: ...

    @property
    def paths(self) -> Paths: ...

    @property
    def spark(self) -> SparkSession: ...

    @property
    def table(self) -> Table: ...

    @property
    def change_data_capture(self) -> AllowedChangeDataCaptures: ...

    @property
    def mode(self) -> AllowedModes: ...

    def __str__(self) -> str: ...


class InvocableJob(Protocol):
    """Narrow interface required by JobInvoker."""

    @property
    def invoker_options(self) -> Optional[InvokerOptions]: ...

    @property
    def step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]: ...

    @property
    def step(self) -> str: ...

    @property
    def topic(self) -> str: ...

    @property
    def item(self) -> str: ...

    @property
    def timeout(self) -> int: ...

    @property
    def options(self) -> TOptions: ...

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]: ...

    def __str__(self) -> str: ...


class StorableCDC(Protocol):
    """Narrow CDC interface required by JobDBA — the 8 methods it actually calls."""

    def drop(self) -> None: ...

    def create_table(self, src: Any, **kwargs) -> None: ...

    def create_or_replace_view(self, src: Any, schema_evolution: bool = True, **kwargs) -> None: ...

    def optimize_table(self) -> None: ...

    def get_schema_differences(self, src: Any, **kwargs) -> Optional[Sequence[SchemaDiff]]: ...

    def get_differences_with_deltatable(self, src: Any, **kwargs) -> Any: ...

    def update_schema(self, src: Any, **kwargs) -> None: ...

    def overwrite_schema(self, src: Any, **kwargs) -> None: ...


class StorableJob(Protocol):
    """Narrow interface required by JobDBA."""

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

    # spark
    @property
    def spark(self) -> SparkSession: ...

    # table & CDC
    @property
    def table(self) -> Table: ...

    @property
    def cdc(self) -> StorableCDC: ...

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

    # methods
    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> CDCIntentContext: ...

    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    def base_transform(self, df: DataFrame) -> DataFrame: ...

    def create_or_replace_view(self) -> None: ...

    def __str__(self) -> str: ...


class RunnableJob(Protocol):
    """Narrow interface required by JobRunner."""

    # state
    @property
    def schema_drift(self) -> bool: ...

    @property
    def persist(self) -> bool: ...

    @property
    def stream(self) -> bool: ...

    @property
    def virtual(self) -> bool: ...

    @property
    def mode(self) -> AllowedModes: ...

    @property
    def paths(self) -> Paths: ...

    @property
    def timeout(self) -> int: ...

    @property
    def table(self) -> Table: ...

    @property
    def options(self) -> TOptions: ...

    # data & transform
    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    def base_transform(self, df: DataFrame) -> DataFrame: ...

    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs) -> None: ...

    def create_or_replace_view(self) -> None: ...

    # schema
    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]: ...

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False) -> None: ...

    # lifecycle
    def restore(self, last_version: Optional[str] = None, last_batch: Optional[str] = None) -> None: ...

    def maintain(self, vacuum: bool = True, optimize: bool = True, compute_statistics: bool = True) -> None: ...

    def __str__(self) -> str: ...


class JobProtocol(CheckableJob, InvocableJob, StorableJob, RunnableJob, Protocol):
    """Full job interface — composition of the four delegate-specific protocols."""
