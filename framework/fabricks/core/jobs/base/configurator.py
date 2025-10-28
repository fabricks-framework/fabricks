from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Optional, Union, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc import SCD1, SCD2, AllowedChangeDataCaptures, NoCDC
from fabricks.context import CONF_RUNTIME, PATHS_RUNTIME, PATHS_STORAGE, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.context.spark_session import build_spark_session
from fabricks.core.jobs.base._types import AllowedModes, Options, Paths, TStep
from fabricks.core.jobs.get_job_conf import get_job_conf
from fabricks.core.jobs.get_job_id import get_job_id
from fabricks.metastore.table import Table
from fabricks.utils.fdict import FDict
from fabricks.utils.path import Path


class Configurator(ABC):
    def __init__(
        self,
        expand: str,
        step: TStep,
        topic: Optional[str] = None,
        item: Optional[str] = None,
        job_id: Optional[str] = None,
        conf: Optional[Union[dict, Row]] = None,
    ):
        self.expand = expand
        self.step: TStep = step

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

    _step_conf: Optional[dict[str, str]] = None
    _spark: Optional[SparkSession] = None
    _timeout: Optional[int] = None
    _options: Optional[Options] = None
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

            step_options = self.step_conf.get("spark_options", {})
            step_sql_options = step_options.get("sql", {})
            step_conf_options = step_options.get("conf", {})
            if step_sql_options:
                for key, value in step_sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.sql(f"set {key} = {value}")
            if step_conf_options:
                for key, value in step_conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self.step})
                    spark.conf.set(f"{key}", f"{value}")

            job_sql_options = self.options.spark.get_dict("sql")
            job_conf_options = self.options.spark.get_dict("conf")
            if job_sql_options:
                for key, value in job_sql_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.sql(f"set {key} = {value}")
            if job_conf_options:
                for key, value in job_conf_options.items():
                    DEFAULT_LOGGER.debug(f"add {key} = {value}", extra={"label": self})
                    spark.conf.set(f"{key}", f"{value}")

            self._spark = spark
        return self._spark

    @property
    def step_conf(self) -> dict:
        if not self._step_conf:
            _conf = [s for s in STEPS if s.get("name") == self.step][0]
            assert _conf is not None
            self._step_conf = cast(dict[str, str], _conf)
        return self._step_conf

    @property
    def qualified_name(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    def _get_timeout(self, what: str) -> int:
        t = self.step_conf.get("options", {}).get("timeouts", {}).get(what, None)
        if t is None:
            t = CONF_RUNTIME.get("options", {}).get("timeouts", {}).get(what)
        assert t is not None
        return t

    @property
    def timeout(self) -> int:
        if not self._timeout:
            t = self.options.job.get("timeout")

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
                storage=storage,
                tmp=storage.joinpath("tmp", self.topic, self.item),
                checkpoints=storage.joinpath("checkpoints", self.topic, self.item),
                commits=storage.joinpath("checkpoints", self.topic, self.item, "commits"),
                schema=storage.joinpath("schema", self.topic, self.item),
                runtime=runtime_root.joinpath(self.topic, self.item),
            )

        return self._paths

    @property
    @lru_cache(maxsize=None)
    def options(self) -> Options:
        if not self._options:
            job = self.conf.options or {}
            table = self.conf.table_options or {}
            check = self.conf.check_options or {}
            spark = self.conf.spark_options or {}
            invokers = self.conf.invoker_options or {}
            extenders = self.conf.extender_options or []

            self._options = Options(
                job=FDict(job),
                table=FDict(table),
                check=FDict(check),
                spark=FDict(spark),
                invokers=FDict(invokers),
                extenders=extenders,
            )
        return self._options

    @property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        if not self._change_data_capture:
            cdc: AllowedChangeDataCaptures = self.options.job.get("change_data_capture") or "nocdc"
            self._change_data_capture = cdc
        return self._change_data_capture

    @property
    def cdc(self) -> Union[NoCDC, SCD1, SCD2]:
        if not self._cdc:
            if self.change_data_capture == "nocdc":
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
            _mode = self.options.job.get("mode")
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
            job = self.options.table.get("retention_days")
            step = self.step_conf.get("table_options", {}).get("retention_days", None)
            runtime = CONF_RUNTIME.get("options", {}).get("retention_days")

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
