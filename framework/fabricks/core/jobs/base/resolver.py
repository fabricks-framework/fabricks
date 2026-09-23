from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context import PATHS_RUNTIME, PATHS_STORAGE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
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
    StepTableOptions,
    TableOptions,
    UpdaterOptions,
)

if TYPE_CHECKING:
    from fabricks.core.jobs.base.job import BaseJob


def resolve_option(*tiers: Any, default: Any = None) -> Any:  # noqa: ANN401 - option values are heterogeneous (str/int/bool/dict depending on the caller)
    """First non-None value across ordered fallback tiers (e.g. job -> step -> runtime), else `default`."""
    for value in tiers:
        if value is not None:
            return value
    return default


class JobResolver:
    """Config/option resolution (job -> step -> runtime) and the spark/cdc/table handles built from it."""

    _spark: SparkSession | None = None  # Keep mutable - has side effects

    def __init__(self, job: BaseJob) -> None:
        self.job = job

    @property
    def spark(self) -> SparkSession:
        if not self._spark:
            spark = build_spark_session(app_name=str(self.job))

            # Apply step-level spark options if configured
            step_spark = self.step_spark_options
            if step_spark:
                sql_options = step_spark.sql or {}
                for key, value in sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.job.step})
                    spark.sql(f"set {key} = {value}")

                conf_options = step_spark.conf or {}
                for key, value in conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.job.step})
                    spark.conf.set(f"{key}", f"{value}")

            # Apply job-level spark options if configured
            job_spark = self.spark_options
            if job_spark:
                sql_options = job_spark.sql or {}
                for key, value in sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.job})
                    spark.sql(f"set {key} = {value}")

                conf_options = job_spark.conf or {}
                for key, value in conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.job})
                    spark.conf.set(f"{key}", f"{value}")

            self._spark = spark
        return self._spark

    @property
    def qualified_name(self) -> str:
        return f"{self.job.step}.{self.job.topic}_{self.job.item}"

    def _get_timeout(self, what: str) -> int:
        t = resolve_option(
            getattr(self.job.step_options.timeouts, what, None), getattr(self.runtime_options.timeouts, what)
        )
        assert t is not None
        return t

    @cached_property
    def timeout(self) -> int:
        t = resolve_option(self.job.options.timeout, self._get_timeout("job"))
        assert t is not None
        return int(t)

    def pip(self) -> None:
        pass

    @property
    def table(self) -> Table:
        return self.cdc.table

    @cached_property
    def paths(self) -> Paths:
        storage = PATHS_STORAGE.get(self.job.step)
        assert storage

        runtime_root = PATHS_RUNTIME.get(self.job.step)
        assert runtime_root

        return Paths(
            to_storage=storage,
            to_tmp=storage.joinpath("tmp", self.job.topic, self.job.item),
            to_checkpoints=storage.joinpath("checkpoints", self.job.topic, self.job.item),
            to_commits=storage.joinpath("checkpoints", self.job.topic, self.job.item, "commits"),
            to_schema=storage.joinpath("schema", self.job.topic, self.job.item),
            to_runtime=runtime_root.joinpath(self.job.topic, self.job.item),
        )

    @cached_property
    def runtime_conf(self) -> RuntimeConf:
        """Direct access to typed runtime conf."""
        from fabricks.context.runtime import CONF_RUNTIME

        return CONF_RUNTIME

    @cached_property
    def step_table_options(self) -> StepTableOptions | None:
        """Direct access to typed step-level table options from context configuration."""
        from fabricks.context import STEPS

        return STEPS[self.job.step].table_options

    @property
    def runtime_options(self) -> RuntimeOptions:
        """Direct access to typed runtime options from context configuration."""
        return self.runtime_conf.options

    @property
    def step_spark_options(self) -> SparkOptions | None:
        """Direct access to typed step-level spark options from context configuration.
        Returns None if not configured at step level."""
        return self.job.step_conf.spark_options

    @property
    def table_options(self) -> TableOptions | None:
        """Direct access to typed table options."""
        return self.job.conf.table_options

    @property
    def check_options(self) -> CheckOptions | None:
        """Direct access to typed check options."""
        return self.job.conf.check_options

    @property
    def spark_options(self) -> SparkOptions | None:
        """Direct access to typed spark options."""
        return self.job.conf.spark_options

    @property
    def invoker_options(self) -> InvokerOptions | None:
        """Direct access to typed invoker options."""
        return self.job.conf.invoker_options

    @property
    def updater_options(self) -> UpdaterOptions | None:
        """Direct access to typed updater options."""
        return self.job.conf.updater_options

    @property
    def extender_options(self) -> list[ExtenderOptions] | None:
        """Direct access to typed extender options."""
        return self.job.conf.extender_options

    @cached_property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        return self.job.options.change_data_capture or "nocdc"

    @cached_property
    def cdc(self) -> NoCDC | SCD0 | SCD1 | SCD2:
        if self.change_data_capture == "nocdc":
            return NoCDC(self.job.step, self.job.topic, self.job.item, spark=self.spark)
        if self.change_data_capture == "scd0":
            return SCD0(self.job.step, self.job.topic, self.job.item, spark=self.spark)
        if self.change_data_capture == "scd1":
            return SCD1(self.job.step, self.job.topic, self.job.item, spark=self.spark)
        if self.change_data_capture == "scd2":
            return SCD2(self.job.step, self.job.topic, self.job.item, spark=self.spark)
        raise ValueError(f"{self.change_data_capture} not allowed")

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd0", "scd1", "scd2"]

    def get_cdc_data(self, stream: bool = False) -> DataFrame | None:
        df = self.job.get_data(stream=stream)
        if df:
            cdc_context = self.job.build_cdc_context(df)
            return self.cdc.get_data(src=df, **cdc_context)
        return None

    @cached_property
    def mode(self) -> AllowedModes:
        _mode = self.job.options.mode
        assert _mode is not None
        return _mode
