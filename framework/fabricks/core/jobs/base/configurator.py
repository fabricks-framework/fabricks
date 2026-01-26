from abc import ABC, abstractmethod
from typing import List, Optional, Union, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.context import CONF_RUNTIME, PATHS_RUNTIME, PATHS_STORAGE, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
from fabricks.core.jobs.get_job_conf import get_job_conf
from fabricks.metastore.table import Table
from fabricks.models import (
    AllowedChangeDataCaptures,
    AllowedModes,
    BronzeConf,
    CheckOptions,
    ExtenderOptions,
    GoldConf,
    InvokerOptions,
    Paths,
    RuntimeOptions,
    SilverConf,
    SparkOptions,
    StepBronzeOptions,
    StepGoldOptions,
    StepSilverOptions,
    StepTableOptions,
    TableOptions,
    TOptions,
    get_job_id,
)
from fabricks.utils.path import Path


class Configurator(ABC):
    def __init__(
        self,
        expand: str,
        step: str,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):
        self.expand = expand
        self.step = step

        if job_id is not None:
            self.job_id = job_id
            self.conf = get_job_conf(step=self.step, job_id=self.job_id, row=conf)
            self.topic = self.conf.topic
            self.item = self.conf.item

        else:
            assert topic
            assert item
            self.topic = topic
            self.item = item
            self.conf = get_job_conf(step=self.step, topic=self.topic, item=self.item, row=conf)
            self.job_id = get_job_id(step=self.step, topic=self.topic, item=self.item)

    _step_conf: Optional[Union[BronzeConf, SilverConf, GoldConf]] = None
    _step_options: Optional[Union[StepBronzeOptions, StepSilverOptions, StepGoldOptions]] = None
    _step_table_options: Optional[StepTableOptions] = None
    _runtime_options: Optional[RuntimeOptions] = None
    _spark: Optional[SparkSession] = None
    _timeout: Optional[int] = None
    _paths: Optional[Paths] = None
    _table: Optional[Table] = None
    _root: Optional[Path] = None

    _cdc: Optional[Union[NoCDC, SCD1, SCD2]] = None
    _change_data_capture: Optional[AllowedChangeDataCaptures] = None
    _mode: Optional[AllowedModes] = None

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

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str): ...

    @classmethod
    def from_job_id(cls, step: str, job_id: str): ...

    @property
    def spark(self) -> SparkSession:
        if not self._spark:
            spark = build_spark_session(app_name=str(self))

            # Apply step-level spark options if configured
            step_spark = self.step_spark_options
            if step_spark:
                sql_options = step_spark.sql or {}
                for key, value in sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.sql(f"set {key} = {value}")

                conf_options = step_spark.conf or {}
                for key, value in conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.conf.set(f"{key}", f"{value}")

            # Apply job-level spark options if configured
            job_spark = self.spark_options
            if job_spark:
                sql_options = job_spark.sql or {}
                for key, value in sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.sql(f"set {key} = {value}")

                conf_options = job_spark.conf or {}
                for key, value in conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.conf.set(f"{key}", f"{value}")

            self._spark = spark
        return self._spark

    @property
    def base_step_conf(self) -> Union[BronzeConf, SilverConf, GoldConf]:
        if not self._step_conf:
            _conf = [s for s in STEPS if s.name == self.step][0]
            assert _conf is not None
            self._step_conf = _conf
        return self._step_conf

    @property
    def qualified_name(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    def _get_timeout(self, what: str) -> int:
        t = getattr(self.step_options.timeouts, what, None)
        if t is None:
            t = getattr(self.runtime_options.timeouts, what)
        assert t is not None
        return t

    @property
    def timeout(self) -> int:
        if not self._timeout:
            t = self.options.timeout

            if t is None:
                t = self._get_timeout("job")

            assert t is not None
            self._timeout = int(t)

        return self._timeout

    def pip(self):
        pass

    @property
    def table(self) -> Table:
        return self.cdc.table

    @property
    def paths(self) -> Paths:
        if not self._paths:
            storage = PATHS_STORAGE.get(self.step)
            assert storage

            runtime_root = PATHS_RUNTIME.get(self.step)
            assert runtime_root

            self._paths = Paths(
                to_storage=storage,
                to_tmp=storage.joinpath("tmp", self.topic, self.item),
                to_checkpoints=storage.joinpath("checkpoints", self.topic, self.item),
                to_commits=storage.joinpath("checkpoints", self.topic, self.item, "commits"),
                to_schema=storage.joinpath("schema", self.topic, self.item),
                to_runtime=runtime_root.joinpath(self.topic, self.item),
            )

        assert self._paths is not None
        return self._paths

    @property
    @abstractmethod
    def options(self) -> TOptions:
        """Direct access to typed job options."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def step_conf(self) -> Union[BronzeConf, SilverConf, GoldConf]:
        """Direct access to typed step conf from context configuration."""
        raise NotImplementedError()

    @property
    def step_options(self) -> Union[StepBronzeOptions, StepSilverOptions, StepGoldOptions]:
        """Direct access to typed step-level options from context configuration."""
        if not self._step_options:
            _step = [s for s in STEPS if s.name == self.step][0]
            assert _step is not None
            self._step_options = _step.options
        return self._step_options

    @property
    def step_table_options(self) -> Optional[StepTableOptions]:
        """Direct access to typed step-level table options from context configuration."""
        if self._step_table_options is None:
            _step = [s for s in STEPS if s.name == self.step][0]
            assert _step is not None
            self._step_table_options = _step.table_options
        return self._step_table_options

    @property
    def runtime_options(self) -> RuntimeOptions:
        """Direct access to typed runtime options from context configuration."""
        if not self._runtime_options:
            self._runtime_options = CONF_RUNTIME.options
        return self._runtime_options

    @property
    def step_spark_options(self) -> Optional[SparkOptions]:
        """Direct access to typed step-level spark options from context configuration.
        Returns None if not configured at step level."""
        return self.step_conf.spark_options

    @property
    def table_options(self) -> Optional[TableOptions]:
        """Direct access to typed table options."""
        return self.conf.table_options

    @property
    def check_options(self) -> Optional[CheckOptions]:
        """Direct access to typed check options."""
        return self.conf.check_options

    @property
    def spark_options(self) -> Optional[SparkOptions]:
        """Direct access to typed spark options."""
        return self.conf.spark_options

    @property
    def invoker_options(self) -> Optional[InvokerOptions]:
        """Direct access to typed invoker options."""
        return self.conf.invoker_options

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]:
        """Direct access to typed extender options."""
        return self.conf.extender_options

    @property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        if not self._change_data_capture:
            cdc: AllowedChangeDataCaptures = self.options.change_data_capture or "nocdc"
            self._change_data_capture = cdc
        return self._change_data_capture

    @property
    def cdc(self) -> Union[NoCDC, SCD1, SCD2]:
        if not self._cdc:
            if self.change_data_capture in ["nocdc", "none"]:
                cdc = NoCDC(self.step, self.topic, self.item, spark=self.spark)
            elif self.change_data_capture == "scd1":
                cdc = SCD1(self.step, self.topic, self.item, spark=self.spark)
            elif self.change_data_capture == "scd2":
                cdc = SCD2(self.step, self.topic, self.item, spark=self.spark)
            else:
                raise ValueError(f"{self.change_data_capture} not allowed")
            self._cdc = cdc
        return self._cdc

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd1", "scd2"]

    @abstractmethod
    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> dict: ...

    def get_cdc_data(self, stream: bool = False) -> Optional[DataFrame]:
        df = self.get_data(stream=stream)
        if df:
            cdc_context = self.get_cdc_context(df)
            cdc_df = self.cdc.get_data(src=df, **cdc_context)
            return cdc_df

    @property
    def mode(self) -> AllowedModes:
        if not self._mode:
            _mode = self.options.mode
            assert _mode is not None
            self._mode = cast(AllowedModes, _mode)
        return self._mode

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

    @deprecated("use maintain instead")
    def optimize(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        analyze: Optional[bool] = True,
    ):
        return self.maintain(
            vacuum=vacuum,
            optimize=optimize,
            compute_statistics=analyze,
        )

    def maintain(
        self,
        vacuum: Optional[bool] = True,
        optimize: Optional[bool] = True,
        compute_statistics: Optional[bool] = True,
    ):
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": self})

        else:
            if vacuum:
                self.vacuum()
            if optimize:
                self.cdc.optimize_table()
            if compute_statistics:
                self.table.compute_statistics()

    def vacuum(self):
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("could not vacuum (memory)", extra={"label": self})

        else:
            job = self.table_options.retention_days if self.table_options else None
            step = self.step_table_options.retention_days if self.step_table_options else None
            runtime = self.runtime_options.retention_days

            if job is not None:
                retention_days = job
            elif step:
                retention_days = step
            else:
                assert runtime
                retention_days = runtime

            self.table.vacuum(retention_days=retention_days)

    def __str__(self):
        return f"{self.step}.{self.topic}_{self.item}"
