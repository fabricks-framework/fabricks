from functools import cached_property
from typing import List, Optional, Union

from pyspark.sql import SparkSession

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
from fabricks.core.jobs.mixins._protocol import JobProtocol
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
    StepGoldConf,
    StepSilverConf,
    StepTableOptions,
    TableOptions,
    UpdaterOptions,
)


class ConfiguratorMixin(JobProtocol):
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
    def base_step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]:
        return self.config.base_step_conf

    @property
    def qualified_name(self) -> str:
        return self.config.qualified_name

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

    def pip(self):
        pass

    @property
    def table(self) -> Table:
        return self.cdc.table

    @property
    def paths(self) -> Paths:
        return self.config.paths

    @property
    def runtime_conf(self) -> RuntimeConf:
        """Direct access to typed runtime conf."""
        return self.config.runtime_conf

    @property
    def step_table_options(self) -> Optional[StepTableOptions]:
        """Direct access to typed step-level table options from context configuration."""
        return self.config.step_table_options

    @property
    def runtime_options(self) -> RuntimeOptions:
        """Direct access to typed runtime options from context configuration."""
        return self.config.runtime_options

    @property
    def step_spark_options(self) -> Optional[SparkOptions]:
        """Direct access to typed step-level spark options from context configuration.
        Returns None if not configured at step level."""
        return self.step_conf.spark_options

    @property
    def table_options(self) -> Optional[TableOptions]:
        return self.config.table_options

    @property
    def check_options(self) -> Optional[CheckOptions]:
        return self.config.check_options

    @property
    def spark_options(self) -> Optional[SparkOptions]:
        return self.config.spark_options

    @property
    def invoker_options(self) -> Optional[InvokerOptions]:
        return self.config.invoker_options

    @property
    def updater_options(self) -> Optional[UpdaterOptions]:
        return self.config.updater_options

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]:
        return self.config.extender_options

    @cached_property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        return self.options.change_data_capture or "nocdc"

    @cached_property
    def cdc(self) -> Union[NoCDC, SCD0, SCD1, SCD2]:
        if self.change_data_capture == "nocdc":
            return NoCDC(self.step, self.topic, self.item, spark=self.spark)
        elif self.change_data_capture == "scd0":
            return SCD0(self.step, self.topic, self.item, spark=self.spark)
        elif self.change_data_capture == "scd1":
            return SCD1(self.step, self.topic, self.item, spark=self.spark)
        elif self.change_data_capture == "scd2":
            return SCD2(self.step, self.topic, self.item, spark=self.spark)
        else:
            raise ValueError(f"{self.change_data_capture} not allowed")

    @property
    def slowly_changing_dimension(self) -> bool:
        return self.change_data_capture in ["scd0", "scd1", "scd2"]

    @cached_property
    def mode(self) -> AllowedModes:
        _mode = self.options.mode
        assert _mode is not None
        return _mode

    def __str__(self) -> str:
        return str(self.config)
