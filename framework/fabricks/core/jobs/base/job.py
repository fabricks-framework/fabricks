from abc import ABC, abstractmethod
from collections.abc import Sequence
from functools import cached_property, partial
from typing import Any, ClassVar, Self

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.cdc.scd0 import SCD0
from fabricks.context import IS_TYPE_WIDENING, STEPS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.checker import JobChecker
from fabricks.core.jobs.base.exception import (
    PostRunCheckException,
    PostRunCheckWarning,
    PostRunInvokeException,
    PreRunCheckException,
    PreRunCheckWarning,
    PreRunInvokeException,
    SchemaDriftError,
    SkipRunCheckWarning,
    SkipRunTimeWarning,
)
from fabricks.core.jobs.base.generator import JobGenerator
from fabricks.core.jobs.base.invoker import JobInvoker
from fabricks.core.jobs.base.resolver import JobResolver
from fabricks.core.jobs.get_job_conf import get_job_conf
from fabricks.metastore.table import Table
from fabricks.models import (
    JobBronzeOptions,
    JobDependency,
    JobSilverOptions,
    StepBronzeConf,
    StepBronzeOptions,
    StepGoldConf,
    StepGoldOptions,
    StepSilverConf,
    StepSilverOptions,
    TOptions,
    get_job_id,
)
from fabricks.utils.write import write_stream


def _for_each_stream_batch(
    df: DataFrame,
    batch: int,
    *,
    step: str,
    topic: str,
    item: str,
    schedule: str | None,
    reload: bool | None,
    conf: dict,
) -> None:
    from fabricks.core.jobs.get_job import get_job_internal

    # conf is the driver's already-resolved JobConf, serialized -- passing it
    # through means get_job_conf() (fabricks/core/jobs/get_job_conf.py) skips
    # its SPARK.sql(f"select * from fabricks.{step}_jobs") lookup and reuses
    # this instead of re-querying the metastore on every stream start.
    job = get_job_internal(step=step, topic=topic, item=item, conf=conf)
    job._for_each_batch(df, batch, schedule=schedule, reload=reload)


class BaseJob(ABC):
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

        self._resolver = JobResolver(self)
        self._checker = JobChecker(self)
        self._invoker = JobInvoker(self)
        self._generator = JobGenerator(self)

    # --- identity / abstract tier hooks ------------------------------------------------

    @cached_property
    def is_stream(self) -> bool:
        return False

    @cached_property
    def is_auto_schema_drift(self) -> bool:
        return True

    # Modes for which this job is a table / a view, resp. Subclasses set these;
    # a mode in neither list means the job is not allowed to run (see for_each_run/restore).
    _table_modes: ClassVar[Sequence[str]] = ()
    _view_modes: ClassVar[Sequence[str]] = ()

    @property
    def is_table(self) -> bool:
        return self._resolver.mode in self._table_modes

    @property
    def is_view(self) -> bool:
        return self._resolver.mode in self._view_modes

    @classmethod
    @abstractmethod
    def from_step_topic_item(cls, step: str, topic: str, item: str) -> Self: ...

    @classmethod
    @abstractmethod
    def from_job_id(cls, step: str, job_id: str) -> Self: ...

    @property
    @abstractmethod
    def options(self) -> TOptions:
        """Direct access to typed job options (e.g. JobBronzeOptions, JobSilverOptions, JobGoldOptions)."""
        ...

    @property
    @abstractmethod
    def step_conf(self) -> StepBronzeConf | StepSilverConf | StepGoldConf:
        """Direct access to typed step conf from context configuration."""
        ...

    @property
    @abstractmethod
    def step_options(self) -> StepBronzeOptions | StepSilverOptions | StepGoldOptions:
        """Direct access to typed step-level options from context configuration."""
        ...

    @cached_property
    def base_step_conf(self) -> StepBronzeConf | StepSilverConf | StepGoldConf:
        return STEPS[self.step]

    @abstractmethod
    def build_cdc_context(self, df: DataFrame, reload: bool | None = False) -> dict: ...

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
    def base_transform(self, df: DataFrame) -> DataFrame: ...

    @abstractmethod
    def get_dependencies(self) -> Sequence[JobDependency]: ...

    @abstractmethod
    def overwrite(self) -> None: ...

    def __str__(self) -> str:
        return f"{self.step}.{self.topic}_{self.item}"

    # --- facades: config/option resolution (JobResolver) ---------------------------------

    @property
    def spark(self) -> SparkSession:
        return self._resolver.spark

    @property
    def qualified_name(self) -> str:
        return self._resolver.qualified_name

    @property
    def table(self) -> Table:
        return self._resolver.table

    @property
    def cdc(self) -> NoCDC | SCD0 | SCD1 | SCD2:
        return self._resolver.cdc

    def get_udfs(self) -> list[str] | None:
        return None

    # --- facades: DDL mechanics (JobGenerator) --------------------------------------------

    def optimize(self, vacuum: bool | None = True, optimize: bool | None = True, analyze: bool | None = True) -> None:
        return self.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=analyze)

    def maintain(
        self, vacuum: bool | None = True, optimize: bool | None = True, compute_statistics: bool | None = True
    ) -> None:
        self._generator.maintain(vacuum=vacuum, optimize=optimize, compute_statistics=compute_statistics)

    def vacuum(self) -> None:
        self._generator.vacuum()

    def truncate(self) -> None:
        self._generator.truncate()

    def drop(self) -> None:
        self._generator.drop()

    def create(self) -> None:
        self._generator.create()

    def register(self) -> None:
        self._generator.register()

    def create_or_replace_view(self) -> None:
        self._generator.create_or_replace_view()

    def update_schema(self, df: DataFrame | None = None, widen_types: bool | None = False) -> None:
        self._generator.update_schema(df=df, widen_types=widen_types)

    def overwrite_schema(self, df: DataFrame | None = None) -> None:
        self._generator.overwrite_schema(df=df)

    # --- facades: checks (JobChecker) -----------------------------------------------------

    def check_pre_run(self) -> None:
        self._checker.pre_run()

    def check_post_run(self) -> None:
        self._checker.post_run()

    # --- composition root: run orchestration (stays directly on Job) ---------------------

    def filter_where(self, df: DataFrame) -> DataFrame:
        assert isinstance(self.options, (JobBronzeOptions, JobSilverOptions))

        f = self.options.filter_where
        if f:
            DEFAULT_LOGGER.debug(f"filter where {f}", extra={"label": self})
            df = df.where(f"{f}")

        return df

    def restore(self, last_version: str | None = None, last_batch: str | None = None) -> None:
        """
        Restores the job to a specific version and batch.

        Args:
            last_version (Optional[str]): The last version to restore to. If None, no version restore
                will be performed.
            last_batch (Optional[str]): The last batch to restore to. If None, no batch restore will be performed.
        """
        if self.is_table:
            if last_version is not None:
                _last_version = int(last_version)
                if self.table.get_last_version() > _last_version:
                    self.table.restore_to_version(_last_version)

            if self.is_stream and last_batch is not None:
                current_batch = int(last_batch) + 1
                self._generator.rm_commit(current_batch)

                assert last_batch == self.table.get_property("fabricks.last_batch")
                assert self._resolver.paths.to_commits.joinpath(last_batch).exists()

    def _for_each_batch(self, df: DataFrame, batch: int | None = None, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline
        DEFAULT_LOGGER.debug("start (for each batch)", extra={"label": self})
        if batch is not None:
            DEFAULT_LOGGER.debug(f"batch {batch}", extra={"label": self})

        df = self.base_transform(df)

        diffs = self._generator.get_schema_differences(df)
        if diffs:
            if self.is_auto_schema_drift or kwargs.get("reload", False):
                DEFAULT_LOGGER.warning("schema drifted", extra={"label": self, "diffs": diffs})
                self.update_schema(df=df)

            else:
                only_type_widening_compatible = all(d.type_widening_compatible for d in diffs if d.status == "changed")
                if only_type_widening_compatible and self.table.type_widening_enabled and IS_TYPE_WIDENING:
                    self.update_schema(df=df, widen_types=True)
                else:
                    raise SchemaDriftError.from_diffs(str(self), diffs)

        self.for_each_batch(df, batch, **kwargs)

        if batch is not None:
            self.table.set_property("fabricks.last_batch", batch)

        self.table.create_restore_point()
        DEFAULT_LOGGER.debug("end (for each batch)", extra={"label": self})

    def for_each_run(self, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline
        DEFAULT_LOGGER.debug("start (for each run)", extra={"label": self})

        if self.is_view:
            self.create_or_replace_view()

        elif self.is_table:
            assert self.table.registered, f"{self} is not registered"

            df = self.get_data(stream=self.is_stream, **kwargs)
            assert df is not None, "no data"

            if self.is_stream:
                DEFAULT_LOGGER.debug("use streaming", extra={"label": self})
                callback = partial(
                    _for_each_stream_batch,
                    step=self.step,
                    topic=self.topic,
                    item=self.item,
                    schedule=kwargs.get("schedule"),
                    reload=kwargs.get("reload"),
                    conf=self.conf.model_dump(),
                )
                write_stream(
                    df,
                    checkpoints_path=self._resolver.paths.to_checkpoints,
                    func=callback,
                    timeout=self._resolver.timeout,
                )
            else:
                self._for_each_batch(df, **kwargs)

        else:
            raise ValueError(f"{self._resolver.mode} - not allowed")

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
        **_kwargs: Any,  # noqa: ANN401 - heterogeneous options bag forwarded through the job run pipeline
    ) -> None:
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

        if self.is_table:
            last_version = self.table.get_property("fabricks.last_version")
            if last_version is not None:
                DEFAULT_LOGGER.debug(f"last version {last_version}", extra={"label": self})
            else:
                last_version = str(self.table.last_version)

            if self.is_stream:
                last_batch = self.table.get_property("fabricks.last_batch")
                if last_batch is not None:
                    DEFAULT_LOGGER.debug(f"last batch {last_batch}", extra={"label": self})

        try:
            DEFAULT_LOGGER.info("start (run)", extra={"label": self})

            if reload:
                DEFAULT_LOGGER.debug("force reload", extra={"label": self})

            if not reload:
                self._checker.run_before()
                self._checker.run_after()

                self._checker.skip_run()

            if invoke:
                self._invoker.invoke_pre_run(schedule=schedule)

            try:
                self._checker.pre_run()
            except PreRunCheckWarning as e:
                exception = e

            self.for_each_run(schedule=schedule, reload=reload)

            try:
                self._checker.post_run()
            except PostRunCheckWarning as e:
                exception = e

            self._checker.post_run_extra()

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
                self.maintain(compute_statistics=compute_statistics, optimize=optimize, vacuum=vacuum)

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
            if not self.is_stream or not retry:
                DEFAULT_LOGGER.exception("fail to run", extra={"label": self})
                self.restore(last_version, last_batch)
                raise e

            DEFAULT_LOGGER.warning("retry to run", extra={"label": self})
            self.run(retry=False, schedule_id=schedule_id, schedule=schedule)
