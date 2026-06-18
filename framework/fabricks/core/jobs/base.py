from __future__ import annotations

import re
from abc import ABC, abstractmethod
from functools import cached_property, partial
from typing import List, Optional, Sequence, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from typing_extensions import deprecated

from fabricks.cdc import SCD1, SCD2, CDCIntentContext, NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context import IS_TYPE_WIDENING
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.config import JobConfig
from fabricks.core.jobs.delegates.checker import (
    JobChecker,
    PostRunCheckException,
    PostRunCheckWarning,
    PreRunCheckException,
    PreRunCheckWarning,
    SkipRunCheckWarning,
    SkipRunTimeWarning,
)
from fabricks.core.jobs.delegates.dba import JobDBA, SchemaDriftException
from fabricks.core.jobs.delegates.invoker import JobInvoker, PostRunInvokeException, PreRunInvokeException
from fabricks.core.udfs import UDF_PREFIX, is_registered, register_udf
from fabricks.metastore.table import SchemaDiff, Table
from fabricks.models import (
    AllowedChangeDataCaptures,
    AllowedModes,
    CheckOptions,
    ExtenderOptions,
    InvokerOptions,
    JobBronzeOptions,
    JobConf,
    JobDependency,
    JobSilverOptions,
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
)
from fabricks.utils.write import write_stream

_UDF_PATTERN = re.compile(rf"(?<={UDF_PREFIX})\w*(?=\()")


class BaseJob(ABC):
    """The base of every concrete job (Bronze/Silver/Gold).

    Composes its configuration substrate (``JobConfig``) and its peripheral
    collaborators (``JobChecker``, ``JobInvoker``). Holds the job's spine —
    table DDL/schema management and the run loop — as template methods that
    concrete jobs override via ``super()``.
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
        self._config = JobConfig(expand, step, topic=topic, item=item, job_id=job_id, conf=conf)
        self._checker = JobChecker(self)
        self._invoker = JobInvoker(self)
        self._dba = JobDBA(self)

    _udf_registered: Optional[bool] = None  # Keep mutable - state flag

    @property
    def config(self) -> JobConfig:
        """The resolved configuration substrate (identity, spark, paths, options)."""
        return self._config

    # --- identity & configuration (delegated to JobConfig) ---

    @property
    def expand(self) -> str:
        return self._config.expand

    @property
    def step(self) -> str:
        return self._config.step

    @property
    def topic(self) -> str:
        return self._config.topic

    @property
    def item(self) -> str:
        return self._config.item

    @property
    def job_id(self) -> str:
        return self._config.job_id

    @property
    def conf(self) -> JobConf:
        return self._config.conf

    @property
    def spark(self) -> SparkSession:
        return self._config.spark

    @property
    def base_step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]:
        return self._config.base_step_conf

    @property
    def qualified_name(self) -> str:
        return self._config.qualified_name

    @property
    def timeout(self) -> int:
        return self._config.timeout

    @property
    def paths(self) -> Paths:
        return self._config.paths

    @property
    def runtime_conf(self) -> RuntimeConf:
        return self._config.runtime_conf

    @property
    def runtime_options(self) -> RuntimeOptions:
        return self._config.runtime_options

    @property
    def step_options(self) -> Union[StepBronzeOptions, StepSilverOptions, StepGoldOptions]:
        return self._config.step_options

    @property
    def step_table_options(self) -> Optional[StepTableOptions]:
        return self._config.step_table_options

    @property
    def step_spark_options(self) -> Optional[SparkOptions]:
        return self._config.step_spark_options

    @property
    def table_options(self) -> Optional[TableOptions]:
        return self._config.table_options

    @property
    def check_options(self) -> Optional[CheckOptions]:
        return self._config.check_options

    @property
    def spark_options(self) -> Optional[SparkOptions]:
        return self._config.spark_options

    @property
    def invoker_options(self) -> Optional[InvokerOptions]:
        return self._config.invoker_options

    @property
    def updater_options(self) -> Optional[UpdaterOptions]:
        return self._config.updater_options

    @property
    def extender_options(self) -> Optional[List[ExtenderOptions]]:
        return self._config.extender_options

    @property
    def change_data_capture(self) -> AllowedChangeDataCaptures:
        return self._config.change_data_capture

    @property
    def mode(self) -> AllowedModes:
        return self._config.mode

    # --- subclass contract (filled by Bronze/Silver/Gold) ---

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
    def options(self) -> TOptions:
        """
        Direct access to typed job options.

        Subclasses must implement this property and return their specific typed
        options instance (e.g. JobBronzeOptions, JobSilverOptions, or JobGoldOptions)
        corresponding to the job type.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def step_conf(self) -> Union[StepBronzeConf, StepSilverConf, StepGoldConf]:
        """Direct access to typed step conf from context configuration."""
        raise NotImplementedError()

    @classmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str): ...

    @classmethod
    def from_job_id(cls, step: str, job_id: str): ...

    def pip(self):
        pass

    # --- CDC strategy & behaviour ---

    @property
    def table(self) -> Table:
        return self.cdc.table

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

    @abstractmethod
    def get_cdc_context(self, df: DataFrame, reload: Optional[bool] = False) -> CDCIntentContext: ...

    def get_cdc_data(self, stream: bool = False) -> Optional[DataFrame]:
        df = self.get_data(stream=stream)
        if df:
            cdc_context = self.get_cdc_context(df)
            cdc_df = self.cdc.get_data(src=df, **cdc_context)
            return cdc_df

    def get_udfs(self) -> Optional[list[str]]:
        updated_columns = self.updater_options.columns if self.updater_options else {}

        if updated_columns:
            udfs = []
            for value in updated_columns.values():
                matches = self._match_udfs(value)
                if matches:
                    udfs += matches

            return list(set(udfs))

    def register_udfs(self, force: bool | None = False):
        if not self._udf_registered or force:
            udfs = self.get_udfs()
            if udfs:
                for u in udfs:
                    if not is_registered(u, self.spark):
                        DEFAULT_LOGGER.debug(f"register udf {u}", extra={"label": self})
                        register_udf(u, spark=self.spark)

            self._udf_registered = True

    def _match_udfs(self, string: str) -> Optional[list[str]]:
        if UDF_PREFIX in string:
            matches = _UDF_PATTERN.findall(string)
            return list(set(matches)) if matches else None

    @abstractmethod
    def get_data(self, stream: bool = False, transform: Optional[bool] = None, **kwargs) -> Optional[DataFrame]: ...

    @abstractmethod
    def for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs): ...

    @abstractmethod
    def base_transform(self, df: DataFrame) -> DataFrame: ...

    # --- DB object lifecycle, schema, maintenance, storage (delegated to JobDBA) ---

    @deprecated("use maintain instead")
    def optimize(self, vacuum=True, optimize=True, analyze=True):
        self._dba.optimize(vacuum=vacuum, optimize=optimize, analyze=analyze)

    def maintain(self, vacuum=True, optimize=True, compute_statistics=True):
        self._dba.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=compute_statistics)

    def vacuum(self):
        self._dba.vacuum()

    def update_dependencies(self):
        self._dba.update_dependencies()

    @abstractmethod
    def get_dependencies(self) -> Sequence[JobDependency]: ...

    def rm(self):
        self._dba.rm()

    def rm_checkpoints(self):
        self._dba.rm_checkpoints()

    def rm_commit(self, id: Union[str, int]):
        self._dba.rm_commit(id)

    def truncate(self):
        self._dba.truncate()

    def drop(self):
        self._dba.drop()

    def create(self):
        self._dba.create()

    def register(self):
        self._dba.register()

    def create_or_replace_view(self): ...

    def update_schema(self, df: Optional[DataFrame] = None, widen_types: Optional[bool] = False):
        self._dba.update_schema(df=df, widen_types=widen_types)

    def overwrite_schema(self, df: Optional[DataFrame] = None):
        self._dba.overwrite_schema(df=df)

    def update_comments(self, table: Optional[bool] = True, columns: Optional[bool] = True):
        self._dba.update_comments(table=table, columns=columns)

    def get_differences_with_deltatable(self, df: Optional[DataFrame] = None):
        return self._dba.get_differences_with_deltatable(df=df)

    def get_schema_differences(self, df: Optional[DataFrame] = None) -> Optional[Sequence[SchemaDiff]]:
        return self._dba.get_schema_differences(df=df)

    def schema_drifted(self, df: Optional[DataFrame] = None) -> Optional[bool]:
        return self._dba.schema_drifted(df=df)

    def _register_external_table(self, file_format: str, uri: str):
        self._dba.register_external_table(file_format=file_format, uri=uri)

    def _drop_external_table(self):
        self._dba.drop_external_table()

    # --- run loop ---

    def filter_where(self, df: DataFrame) -> DataFrame:
        assert isinstance(self.options, (JobBronzeOptions, JobSilverOptions))

        f = self.options.filter_where
        if f:
            DEFAULT_LOGGER.debug(f"filter where {f}", extra={"label": self})
            df = df.where(f"{f}")

        return df

    def restore(self, last_version: str | None = None, last_batch: str | None = None):
        self._dba.restore(last_version, last_batch)

    def _for_each_batch(self, df: DataFrame, batch: int | None = None, **kwargs):
        DEFAULT_LOGGER.debug("start (for each batch)", extra={"label": self})
        if batch is not None:
            DEFAULT_LOGGER.debug(f"batch {batch}", extra={"label": self})

        df = self.base_transform(df)

        diffs = self.get_schema_differences(df)
        if diffs:
            if self.schema_drift or kwargs.get("reload", False):
                DEFAULT_LOGGER.warning("schema drifted", extra={"label": self, "diffs": diffs})
                self.update_schema(df=df)

            else:
                only_type_widening_compatible = all(d.type_widening_compatible for d in diffs if d.status == "changed")
                if only_type_widening_compatible and self.table.type_widening_enabled and IS_TYPE_WIDENING:
                    self.update_schema(df=df, widen_types=True)
                else:
                    raise SchemaDriftException.from_diffs(str(self), diffs)

        self.for_each_batch(df, batch, **kwargs)

        if batch is not None:
            self.table.set_property("fabricks.last_batch", batch)

        self.table.create_restore_point()
        DEFAULT_LOGGER.debug("end (for each batch)", extra={"label": self})

    def for_each_run(self, **kwargs):
        DEFAULT_LOGGER.debug("start (for each run)", extra={"label": self})

        if self.virtual:
            self.create_or_replace_view()

        elif self.persist:
            assert self.table.registered, f"{self} is not registered"

            df = self.get_data(stream=self.stream, **kwargs)
            assert df is not None, "no data"

            partial(self._for_each_batch, **kwargs)

            if self.stream:
                DEFAULT_LOGGER.debug("use streaming", extra={"label": self})
                write_stream(
                    df,
                    checkpoints_path=self.paths.to_checkpoints,
                    func=self._for_each_batch,
                    timeout=self.timeout,
                )
            else:
                self._for_each_batch(df, **kwargs)

        else:
            raise ValueError(f"{self.mode} - not allowed")

        DEFAULT_LOGGER.debug("end (for each run)", extra={"label": self})

    def run(
        self,
        retry: bool | None = True,
        schedule: str | None = None,
        schedule_id: str | None = None,
        invoke: bool | None = True,
        reload: bool | None = None,
        vacuum: bool | None = None,
        optimize: bool | None = None,
        compute_statistics: bool | None = None,
        **kwargs,
    ):
        """
        Run the job.

        Args:
            retry (bool, optional): Whether to retry the execution in case of failure. Defaults to True.
            schedule (str, optional): The schedule to run the job on. Defaults to None.
            schedule_id (str, optional): The ID of the schedule. Defaults to None.
            invoke (bool, optional): Whether to invoke pre-run and post-run methods. Defaults to True.
        """
        last_version = None
        last_batch = None
        exception = None

        if self.persist:
            last_version = self.table.get_property("fabricks.last_version")
            if last_version is not None:
                DEFAULT_LOGGER.debug(f"last version {last_version}", extra={"label": self})
            else:
                last_version = str(self.table.last_version)

            if self.stream:
                last_batch = self.table.get_property("fabricks.last_batch")
                if last_batch is not None:
                    DEFAULT_LOGGER.debug(f"last batch {last_batch}", extra={"label": self})

        try:
            DEFAULT_LOGGER.info("start (run)", extra={"label": self})

            if reload:
                DEFAULT_LOGGER.debug("force reload", extra={"label": self})

            if not reload:
                self._checker.check_run_before()
                self._checker.check_run_after()

                self._checker.check_skip_run()

            if invoke:
                self._invoker.invoke_pre_run(schedule=schedule)

            try:
                self._checker.check_pre_run()
            except PreRunCheckWarning as e:
                exception = e

            self.for_each_run(schedule=schedule, reload=reload)

            try:
                self._checker.check_post_run()
            except PostRunCheckWarning as e:
                exception = e

            self._checker.check_post_run_extra()

            if invoke:
                self._invoker.invoke_post_run(schedule=schedule)

            if exception:
                raise exception

            if vacuum is None:
                vacuum = self.options.vacuum if self.options and self.options.vacuum is not None else False
            if optimize is None:
                optimize = self.options.optimize if self.options and self.options.optimize is not None else False
            if compute_statistics is None:
                compute_statistics = (
                    self.options.compute_statistics
                    if self.options and self.options.compute_statistics is not None
                    else False
                )

            if vacuum or optimize or compute_statistics:
                self.maintain(
                    compute_statistics=compute_statistics,
                    optimize=optimize,
                    vacuum=vacuum,
                )

            DEFAULT_LOGGER.info("end (run)", extra={"label": self})

        except SkipRunCheckWarning as e:
            DEFAULT_LOGGER.warning("skip run", extra={"label": self})
            raise e

        except SkipRunTimeWarning as e:
            DEFAULT_LOGGER.warning("fail to pass time check", extra={"label": self})
            raise e

        except (PreRunCheckWarning, PostRunCheckWarning) as e:
            DEFAULT_LOGGER.warning("fail to pass warning check", extra={"label": self})
            raise e

        except (PreRunInvokeException, PostRunInvokeException) as e:
            DEFAULT_LOGGER.exception("fail to run invoker", extra={"label": self})
            raise e

        except (PreRunCheckException, PostRunCheckException) as e:
            DEFAULT_LOGGER.exception("fail to pass check", extra={"label": self})
            self.restore(last_version, last_batch)
            raise e

        except AssertionError as e:
            DEFAULT_LOGGER.exception("fail to run", extra={"label": self})
            self.restore(last_version, last_batch)
            raise e

        except Exception as e:
            if not self.stream or not retry:
                DEFAULT_LOGGER.exception("fail to run", extra={"label": self})
                self.restore(last_version, last_batch)
                raise e

            else:
                DEFAULT_LOGGER.warning("retry to run", extra={"label": self})
                self.run(retry=False, schedule_id=schedule_id, schedule=schedule)

    @abstractmethod
    def overwrite(self) -> None: ...

    # --- duplicate checks & invocation (delegated to collaborators) ---

    def check_duplicate_key(self):
        self._checker.check_duplicate_key()

    def check_duplicate_hash(self):
        self._checker.check_duplicate_hash()

    def check_duplicate_identity(self):
        self._checker.check_duplicate_identity()

    def invoke(self, schedule: Optional[str] = None, **kwargs):
        return self._invoker.invoke(schedule=schedule, **kwargs)

    def extend(self, df: DataFrame) -> DataFrame:
        return self._invoker.extend(df)

    def __str__(self):
        return str(self._config)
