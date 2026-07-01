from abc import ABC, abstractmethod
from typing import Optional, Union

from pyspark.sql import DataFrame

from fabricks.models import (
    StepBronzeConf,
    StepBronzeOptions,
    StepGoldConf,
    StepGoldOptions,
    StepSilverConf,
    StepSilverOptions,
    TOptions,
)
from fabricks.models.cdc import CdcContext


class JobABC(ABC):
    @property
    @abstractmethod
    def stream(self) -> bool: ...

    @property
    @abstractmethod
    def schema_drift(self) -> bool: ...

    @property
    @abstractmethod
    def persist(self) -> bool: ...

    @property
    @abstractmethod
    def virtual(self) -> bool: ...

    @property
    @abstractmethod
    def options(self) -> TOptions: ...

    @property
    @abstractmethod
    def step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]: ...

    @property
    @abstractmethod
    def step_options(self) -> Union[StepBronzeOptions, StepSilverOptions, StepGoldOptions]: ...

    @abstractmethod
    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> CdcContext: ...

    @abstractmethod
    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    @abstractmethod
    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs): ...

    @abstractmethod
    def for_each_run(self, **kwargs): ...

    @abstractmethod
    def base_transform(self, df: DataFrame) -> DataFrame: ...

    @abstractmethod
    def run(
        self,
        retry: Optional[bool] = True,
        schedule: Optional[str] = None,
        schedule_id: Optional[str] = None,
        invoke: Optional[bool] = True,
    ): ...
