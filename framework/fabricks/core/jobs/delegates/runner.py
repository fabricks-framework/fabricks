from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame

from fabricks.context import IS_TYPE_WIDENING
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.delegates.checker import (
    JobChecker,
    PostRunCheckException,
    PostRunCheckWarning,
    PreRunCheckException,
    PreRunCheckWarning,
    SkipRunCheckWarning,
    SkipRunTimeWarning,
)
from fabricks.core.jobs.delegates.dba import SchemaDriftException
from fabricks.core.jobs.delegates.invoker import JobInvoker, PostRunInvokeException, PreRunInvokeException
from fabricks.core.jobs.protocols import RunnableJob
from fabricks.utils.write import write_stream


class JobRunner:
    """Orchestrates the job run lifecycle: checks → invoke → for_each_run → checks → invoke → maintenance."""

    def __init__(self, job: RunnableJob, checker: JobChecker, invoker: JobInvoker):
        self._job = job
        self._checker = checker
        self._invoker = invoker

    def run(
        self,
        retry: Optional[bool] = True,
        schedule: Optional[str] = None,
        schedule_id: Optional[str] = None,
        invoke: Optional[bool] = True,
        reload: Optional[bool] = None,
        vacuum: Optional[bool] = None,
        optimize: Optional[bool] = None,
        compute_statistics: Optional[bool] = None,
        **kwargs,
    ):
        last_version = None
        last_batch = None
        exception = None

        if self._job.persist:
            last_version = self._job.table.get_property("fabricks.last_version")
            if last_version is not None:
                DEFAULT_LOGGER.debug(f"last version {last_version}", extra={"label": self._job})
            else:
                last_version = str(self._job.table.last_version)

            if self._job.stream:
                last_batch = self._job.table.get_property("fabricks.last_batch")
                if last_batch is not None:
                    DEFAULT_LOGGER.debug(f"last batch {last_batch}", extra={"label": self._job})

        try:
            DEFAULT_LOGGER.info("start (run)", extra={"label": self._job})

            if reload:
                DEFAULT_LOGGER.debug("force reload", extra={"label": self._job})

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

            opts = self._job.options
            if vacuum is None:
                vacuum = opts.vacuum if opts and opts.vacuum is not None else False
            if optimize is None:
                optimize = opts.optimize if opts and opts.optimize is not None else False
            if compute_statistics is None:
                compute_statistics = opts.compute_statistics if opts and opts.compute_statistics is not None else False

            if vacuum or optimize or compute_statistics:
                self._job.maintain(
                    compute_statistics=compute_statistics,
                    optimize=optimize,
                    vacuum=vacuum,
                )

            DEFAULT_LOGGER.info("end (run)", extra={"label": self._job})

        except SkipRunCheckWarning as e:
            DEFAULT_LOGGER.warning("skip run", extra={"label": self._job})
            raise e

        except SkipRunTimeWarning as e:
            DEFAULT_LOGGER.warning("fail to pass time check", extra={"label": self._job})
            raise e

        except (PreRunCheckWarning, PostRunCheckWarning) as e:
            DEFAULT_LOGGER.warning("fail to pass warning check", extra={"label": self._job})
            raise e

        except (PreRunInvokeException, PostRunInvokeException) as e:
            DEFAULT_LOGGER.exception("fail to run invoker", extra={"label": self._job})
            raise e

        except (PreRunCheckException, PostRunCheckException) as e:
            DEFAULT_LOGGER.exception("fail to pass check", extra={"label": self._job})
            self._job.restore(last_version, last_batch)
            raise e

        except AssertionError as e:
            DEFAULT_LOGGER.exception("fail to run", extra={"label": self._job})
            self._job.restore(last_version, last_batch)
            raise e

        except Exception as e:
            if not self._job.stream or not retry:
                DEFAULT_LOGGER.exception("fail to run", extra={"label": self._job})
                self._job.restore(last_version, last_batch)
                raise e
            else:
                DEFAULT_LOGGER.warning("retry to run", extra={"label": self._job})
                self.run(retry=False, schedule_id=schedule_id, schedule=schedule)

    def _for_each_batch(self, df: DataFrame, batch: int | None = None, **kwargs):
        DEFAULT_LOGGER.debug("start (for each batch)", extra={"label": self._job})
        if batch is not None:
            DEFAULT_LOGGER.debug(f"batch {batch}", extra={"label": self._job})

        df = self._job.base_transform(df)

        diffs = self._job.get_schema_differences(df)
        if diffs:
            if self._job.schema_drift or kwargs.get("reload", False):
                DEFAULT_LOGGER.warning("schema drifted", extra={"label": self._job, "diffs": diffs})
                self._job.update_schema(df=df)
            else:
                only_type_widening_compatible = all(d.type_widening_compatible for d in diffs if d.status == "changed")
                if only_type_widening_compatible and self._job.table.type_widening_enabled and IS_TYPE_WIDENING:
                    self._job.update_schema(df=df, widen_types=True)
                else:
                    raise SchemaDriftException.from_diffs(str(self._job), diffs)

        self._job.for_each_batch(df, batch, **kwargs)

        if batch is not None:
            self._job.table.set_property("fabricks.last_batch", batch)

        self._job.table.create_restore_point()
        DEFAULT_LOGGER.debug("end (for each batch)", extra={"label": self._job})

    def for_each_run(self, **kwargs):
        DEFAULT_LOGGER.debug("start (for each run)", extra={"label": self._job})

        if self._job.virtual:
            self._job.create_or_replace_view()

        elif self._job.persist:
            assert self._job.table.registered, f"{self._job} is not registered"

            df = self._job.get_data(stream=self._job.stream, **kwargs)
            assert df is not None, "no data"

            if self._job.stream:
                DEFAULT_LOGGER.debug("use streaming", extra={"label": self._job})
                write_stream(
                    df,
                    checkpoints_path=self._job.paths.to_checkpoints,
                    func=self._for_each_batch,
                    timeout=self._job.timeout,
                )
            else:
                self._for_each_batch(df, **kwargs)

        else:
            raise ValueError(f"{self._job.mode} - not allowed")

        DEFAULT_LOGGER.debug("end (for each run)", extra={"label": self._job})
