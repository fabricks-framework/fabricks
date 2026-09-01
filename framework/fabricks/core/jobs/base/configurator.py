from abc import ABC, abstractmethod
from functools import cached_property
import re
from typing import Any, Self

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context import PATHS_RUNTIME, PATHS_STORAGE, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
from fabricks.core.jobs.get_job_conf import get_job_conf
from fabricks.core.udfs import UDF_PREFIX, is_registered, register_udf
from fabricks.metastore.table import Table
from fabricks.models import (
    AllowedChangeDataCaptures,
    AllowedModes,
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
    Paths,
    RuntimeConf,
    RuntimeOptions,
    SparkOptions,
    StepBronzeConf,
    StepBronzeOptions,
    StepGoldConf,
    StepGoldOptions,
    StepSilverConf,
    StepSilverOptions,
    StepTableOptions,
    TableOptions,
    TOptions,
    UpdaterOptions,
    get_job_id,
)

_UDF_PATTERN = re.compile(rf"(?<={UDF_PREFIX})\w*(?=\()")


class Configurator(ABC):
    def __init__(
        self,
        expand: str,
        step: str,
        topic: str | None = None,
        item: str | None = None,
        job_id: str | None = None,
        conf: dict | Row | None = None,
    ) -> None:
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

    _spark: SparkSession | None = None  # Keep mutable - has side effects
    _udf_registered: bool | None = None  # Keep mutable - state flag

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
    @abstractmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str) -> Self: ...

    @classmethod
    @abstractmethod
    def from_job_id(cls, step: str, job_id: str) -> Self: ...

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

    @cached_property
    def base_step_conf(self) -> StepBronzeConf | StepSilverConf | StepGoldConf:
        return STEPS[self.step]

    @property
    def qualified_name(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    def _get_timeout(self, what: str) -> int:
        t = getattr(self.step_options.timeouts, what, None)
        if t is None:
            t = getattr(self.runtime_options.timeouts, what)
        assert t is not None
        return t

    @cached_property
    def timeout(self) -> int:
        t = self.options.timeout
        if t is None:
            t = self._get_timeout("job")
        assert t is not None
        return int(t)

    def pip(self) -> None:  # noqa: B027 - intentional no-op default, not required to be overridden by subclasses
        pass

    @property
    def table(self) -> Table:
        return self.cdc.table

    @cached_property
    def paths(self) -> Paths:
        storage = PATHS_STORAGE.get(self.step)
        assert storage

        runtime_root = PATHS_RUNTIME.get(self.step)
        assert runtime_root

        return Paths(
            to_storage=storage,
            to_tmp=storage.joinpath("tmp", self.topic, self.item),
            to_checkpoints=storage.joinpath("checkpoints", self.topic, self.item),
            to_commits=storage.joinpath("checkpoints", self.topic, self.item, "commits"),
            to_schema=storage.joinpath("schema", self.topic, self.item),
            to_runtime=runtime_root.joinpath(self.topic, self.item),
        )

    @property
    @abstractmethod
    def options(self) -> TOptions:
        """
        Direct access to typed job options.

        Subclasses must implement this property and return their specific typed
        options instance (e.g. JobBronzeOptions, JobSilverOptions, or JobGoldOptions)
        corresponding to the job type.
        """
        raise NotImplementedError

    @cached_property
    def runtime_conf(self) -> RuntimeConf:
        """Direct access to typed runtime conf."""
        from fabricks.context.runtime import CONF_RUNTIME

        return CONF_RUNTIME

    @property
    @abstractmethod
    def step_conf(self) -> StepBronzeConf | StepSilverConf | StepGoldConf:
        """Direct access to typed step conf from context configuration."""
        raise NotImplementedError

    @property
    def step_options(self) -> StepBronzeOptions | StepSilverOptions | StepGoldOptions:
        """Direct access to typed step-level options from context configuration."""
        raise NotImplementedError

    @cached_property
    def step_table_options(self) -> StepTableOptions | None:
        """Direct access to typed step-level table options from context configuration."""
        return STEPS[self.step].table_options

    @property
    def runtime_options(self) -> RuntimeOptions:
        """Direct access to typed runtime options from context configuration."""
        return self.runtime_conf.options

    @property
    def step_spark_options(self) -> SparkOptions | None:
        """Direct access to typed step-level spark options from context configuration.
        Returns None if not configured at step level."""
        return self.step_conf.spark_options

    @property
    def table_options(self) -> TableOptions | None:
        """Direct access to typed table options."""
        return self.conf.table_options

    @property
    def check_options(self) -> CheckOptions | None:
        """Direct access to typed check options."""
        return self.conf.check_options

    @property
    def spark_options(self) -> SparkOptions | None:
        """Direct access to typed spark options."""
        return self.conf.spark_options

    @property
    def invoker_options(self) -> InvokerOptions | None:
        """Direct access to typed invoker options."""
        return self.conf.invoker_options

    @property
    def updater_options(self) -> UpdaterOptions | None:
        """Direct access to typed updater options."""
        return self.conf.updater_options

    @property
    def extender_options(self) -> list[ExtenderOptions] | None:
        """Direct access to typed extender options."""
        return self.conf.extender_options

    @cached_property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        return self.options.change_data_capture or "nocdc"

    @cached_property
    def cdc(self) -> NoCDC | SCD0 | SCD1 | SCD2:
        if self.change_data_capture == "nocdc":
            return NoCDC(self.step, self.topic, self.item, spark=self.spark)
        if self.change_data_capture == "scd0":
            return SCD0(self.step, self.topic, self.item, spark=self.spark)
        if self.change_data_capture == "scd1":
            return SCD1(self.step, self.topic, self.item, spark=self.spark)
        if self.change_data_capture == "scd2":
            return SCD2(self.step, self.topic, self.item, spark=self.spark)
        raise ValueError(f"{self.change_data_capture} not allowed")

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd0", "scd1", "scd2"]

    @abstractmethod
    def get_cdc_context(self, df: DataFrame, reload: bool | None = False) -> dict: ...

    def get_cdc_data(self, stream: bool = False) -> DataFrame | None:
        df = self.get_data(stream=stream)
        if df:
            cdc_context = self.get_cdc_context(df)
            return self.cdc.get_data(src=df, **cdc_context)
        return None

    @cached_property
    def mode(self) -> AllowedModes:
        _mode = self.options.mode
        assert _mode is not None
        return _mode

    def get_udfs(self) -> list[str] | None:
        updated_columns = self.updater_options.columns if self.updater_options else {}

        if updated_columns:
            udfs = []
            for value in updated_columns.values():
                matches = self._match_udfs(value)
                if matches:
                    udfs += matches

            return list(set(udfs))
        return None

    def register_udfs(self, force: bool | None = False) -> None:
        if not self._udf_registered or force:
            udfs = self.get_udfs()
            if udfs:
                for u in udfs:
                    if not is_registered(u, self.spark):
                        DEFAULT_LOGGER.debug(f"register udf {u}", extra={"label": self})
                        register_udf(u, spark=self.spark)

            self._udf_registered = True

    def _match_udfs(self, string: str) -> list[str] | None:
        if UDF_PREFIX in string:
            matches = _UDF_PATTERN.findall(string)
            return list(set(matches)) if matches else None
        return None

    @abstractmethod
    def get_data(
        self,
        stream: bool = False,
        transform: bool | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline
    ) -> DataFrame | None: ...

    @abstractmethod
    def for_each_batch(
        self,
        df: DataFrame,
        batch: int | None = None,
        **kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline
    ) -> None: ...

    @abstractmethod
    def for_each_run(self, **kwargs: Any) -> None: ...  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline

    @abstractmethod
    def base_transform(self, df: DataFrame) -> DataFrame: ...

    @abstractmethod
    def run(
        self,
        retry: bool | None = True,
        schedule: str | None = None,
        schedule_id: str | None = None,
        invoke: bool | None = True,
    ) -> None: ...

    @deprecated("use maintain instead")
    def optimize(self, vacuum: bool | None = True, optimize: bool | None = True, analyze: bool | None = True) -> None:
        return self.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=analyze)

    def maintain(
        self, vacuum: bool | None = True, optimize: bool | None = True, compute_statistics: bool | None = True
    ) -> None:
        if self.mode == "memory":
            DEFAULT_LOGGER.debug("could not maintain (memory)", extra={"label": self})

        else:
            if vacuum:
                self.vacuum()
            if optimize:
                self.cdc.optimize_table()
            if compute_statistics:
                self.table.compute_statistics()

    def vacuum(self) -> None:
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

    def __str__(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"
