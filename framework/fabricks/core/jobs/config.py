from __future__ import annotations

from functools import cached_property
from typing import List, Optional, Union

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from fabricks.context import PATHS_RUNTIME, PATHS_STORAGE, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
from fabricks.core.jobs.get_job_conf import get_job_conf
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


class JobConfig:
    """Resolved configuration substrate for a job.

    Holds identity, the resolved ``conf``, the lazily built Spark session,
    ``paths``, ``timeout``, and the typed option accessors. Holds no behaviour
    and keeps no reference back to the job — everything depends on it; it
    depends on nothing. Constructable in a test directly from a ``JobConf``.
    """

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
        self._spark: Optional[SparkSession] = None

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

    @property
    def qualified_name(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    @cached_property
    def base_step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]:
        return STEPS[self.step]

    @property
    def options(self) -> TOptions:
        return self.conf.options

    @property
    def step_options(self) -> Union[StepBronzeOptions, StepSilverOptions, StepGoldOptions]:
        return self.base_step_conf.options

    @cached_property
    def runtime_conf(self) -> RuntimeConf:
        from fabricks.context.runtime import CONF_RUNTIME

        return CONF_RUNTIME

    @property
    def runtime_options(self) -> RuntimeOptions:
        return self.runtime_conf.options

    @cached_property
    def step_table_options(self) -> Optional[StepTableOptions]:
        return STEPS[self.step].table_options

    @property
    def step_spark_options(self) -> Optional[SparkOptions]:
        return self.base_step_conf.spark_options

    @property
    def table_options(self) -> Optional[TableOptions]:
        return self.conf.table_options

    @property
    def check_options(self) -> Optional[CheckOptions]:
        return self.conf.check_options

    @property
    def spark_options(self) -> Optional[SparkOptions]:
        return self.conf.spark_options

    @property
    def invoker_options(self) -> Optional[InvokerOptions]:
        return self.conf.invoker_options

    @property
    def updater_options(self) -> Optional[UpdaterOptions]:
        return self.conf.updater_options

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]:
        return self.conf.extender_options

    @cached_property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        return self.options.change_data_capture or "nocdc"

    @cached_property
    def mode(self) -> AllowedModes:
        _mode = self.options.mode
        assert _mode is not None
        return _mode

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
    def spark(self) -> SparkSession:
        if not self._spark:
            spark = build_spark_session(app_name=str(self))

            # Apply step-level spark options if configured
            step_spark = self.step_spark_options
            if step_spark:
                for key, value in (step_spark.sql or {}).items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.sql(f"set {key} = {value}")
                for key, value in (step_spark.conf or {}).items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.conf.set(f"{key}", f"{value}")

            # Apply job-level spark options if configured
            job_spark = self.spark_options
            if job_spark:
                for key, value in (job_spark.sql or {}).items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.sql(f"set {key} = {value}")
                for key, value in (job_spark.conf or {}).items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.conf.set(f"{key}", f"{value}")

            self._spark = spark
        return self._spark

    def __str__(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"
