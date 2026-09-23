from __future__ import annotations

from collections.abc import Sequence
import datetime
from typing import TYPE_CHECKING, Literal

from fabricks.context import TIMEZONE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.exception import (
    CheckError,
    PostRunCheckException,
    PostRunCheckWarning,
    PreRunCheckException,
    PreRunCheckWarning,
    SkipRunCheckWarning,
    SkipRunTimeWarning,
)

if TYPE_CHECKING:
    from pyspark.sql import Row

    from fabricks.core.jobs.base.job import BaseJob


def decide(
    position: Literal["pre_run", "post_run"], fail_rows: Sequence[Row], warning_rows: Sequence[Row]
) -> CheckError | None:
    """Row-to-exception decision behind pre_run/post_run, isolated from the producing Spark query."""
    if fail_rows:
        message = fail_rows[-1]["__message"]
        return PreRunCheckException(message) if position == "pre_run" else PostRunCheckException(message)

    if warning_rows:
        message = warning_rows[-1]["__message"]
        return PreRunCheckWarning(message) if position == "pre_run" else PostRunCheckWarning(message)

    return None


class JobChecker:
    """Pre/post-run check, skip-run and run-time-window decisions."""

    def __init__(self, job: BaseJob) -> None:
        self.job = job

    def pre_run(self) -> None:
        self._check("pre_run")

    def post_run(self) -> None:
        self._check("post_run")

    def _check(self, position: Literal["pre_run", "post_run"]) -> None:
        if self.job._resolver.check_options and getattr(self.job._resolver.check_options, position):
            DEFAULT_LOGGER.debug(f"check {position}", extra={"label": self.job})

            p = self.job._resolver.paths.to_runtime.append(f".{position}.sql")
            assert p.exists(), f"{position} check not found ({p})"

            df = self.job.spark.sql(p.get_sql())

            # Collect once to avoid double scan
            fail_rows = df.where("__action == 'fail'").collect()
            for row in fail_rows:
                DEFAULT_LOGGER.warning(f"check {position} failed due to {row['__message']}", extra={"label": self.job})

            # only spend a second scan on warnings when nothing already failed
            warning_rows: Sequence[Row] = [] if fail_rows else df.where("__action == 'warning'").collect()
            for row in warning_rows:
                DEFAULT_LOGGER.warning(f"check {position} failed due to {row['__message']}", extra={"label": self.job})

            exception = decide(position, fail_rows, warning_rows)
            if exception is not None:
                exception.dataframe = df
                raise exception

    def batch_has_data(self, sql: str) -> bool:
        min_rows = self.job._resolver.check_options.min_rows if self.job._resolver.check_options else None
        if min_rows == 0:
            DEFAULT_LOGGER.debug("check min rows is 0, skipping check", extra={"label": self.job})
            return True

        if self.job.spark.sql(sql).isEmpty():
            DEFAULT_LOGGER.warning("no data", extra={"label": self.job})
            return False

        return True

    def post_run_extra(self) -> None:
        check_options = self.job._resolver.check_options
        min_rows = check_options.min_rows if check_options else None
        max_rows = check_options.max_rows if check_options else None
        count_must_equal = check_options.count_must_equal if check_options else None

        if min_rows or max_rows or count_must_equal:
            df = self.job.spark.sql(f"select count(*) from {self.job}")
            rows = df.collect()[0][0]
            if min_rows:
                DEFAULT_LOGGER.debug("check min rows", extra={"label": self.job})
                if rows < min_rows:
                    raise PostRunCheckException(f"min rows check failed ({rows} < {min_rows})", dataframe=df)

            if max_rows:
                DEFAULT_LOGGER.debug("check max rows", extra={"label": self.job})
                if rows > max_rows:
                    raise PostRunCheckException(f"max rows check failed ({rows} > {max_rows})", dataframe=df)

            if count_must_equal:
                DEFAULT_LOGGER.debug("check count must equal", extra={"label": self.job})
                equals_rows = self.job.spark.read.table(count_must_equal).count()
                if rows != equals_rows:
                    raise PostRunCheckException(
                        f"count must equal check failed ({count_must_equal} - {rows} != {equals_rows})", dataframe=df
                    )

    def _duplicate_in_column(self, column: str) -> None:
        if column in self.job.table.columns:
            DEFAULT_LOGGER.debug(f"check duplicate in {column}", extra={"label": self.job})

            cols = [column]

            if "__source" in self.job.table.columns:
                cols.append("__source")

            if self.job._resolver.change_data_capture == "scd2":
                cols.append("__valid_to")

            elif self.job._resolver.change_data_capture == "nocdc":
                if "__valid_to" in self.job.table.columns:
                    cols.append("__valid_to")
                elif self.job._resolver.mode == "append" and "__timestamp" in self.job.table.columns:
                    cols.append("__timestamp")

            cols = ", ".join(cols)
            df = self.job.spark.sql(f"select {cols} from {self.job} group by all having count(*) > 1 limit 5")

            # Collect once to avoid double scan
            duplicate_rows = df.collect()
            if duplicate_rows:
                duplicates = ",".join([str(row[column]) for row in duplicate_rows])
                raise PostRunCheckException(f"duplicate {column} check failed ({duplicates})", dataframe=df)

        else:
            DEFAULT_LOGGER.debug(f"could not find {column}", extra={"label": self.job})

    def duplicate_key(self) -> None:
        self._duplicate_in_column("__key")

    def duplicate_hash(self) -> None:
        self._duplicate_in_column("__hash")

    def duplicate_identity(self) -> None:
        self._duplicate_in_column("__identity")

    def skip_run(self) -> None:
        if self.job._resolver.check_options and self.job._resolver.check_options.skip:
            DEFAULT_LOGGER.debug("check if run should be skipped", extra={"label": self.job})

            p = self.job._resolver.paths.to_runtime.append(".skip.sql")
            assert p.exists(), "skip check not found"

            df = self.job.spark.sql(p.get_sql())
            skip_df = df.where("__skip")

            # Collect once to avoid double scan
            skip_rows = skip_df.collect()
            if skip_rows:
                for row in skip_rows:
                    DEFAULT_LOGGER.warning(f"skip run due to {row['__message']}", extra={"label": self.job})

                raise SkipRunCheckWarning(skip_rows[-1]["__message"], dataframe=df)

    def run_before(self) -> None:
        if self.job._resolver.check_options and self.job._resolver.check_options.before:
            self._run_time(self.job._resolver.check_options.before, "before")

    def run_after(self) -> None:
        if self.job._resolver.check_options and self.job._resolver.check_options.after:
            self._run_time(self.job._resolver.check_options.after, "after")

    def _run_time(self, time: str, when: Literal["before", "after"]) -> None:
        now = datetime.datetime.now(tz=TIMEZONE)
        time_as_time = datetime.datetime.strptime(time, "%H:%M:%S").time()  # noqa: DTZ007 - only the naive time-of-day is used, combined with tzinfo below
        target = datetime.datetime.combine(now.date(), time_as_time, tzinfo=TIMEZONE)

        DEFAULT_LOGGER.debug(f"check {when} {target}", extra={"label": self.job})

        if when == "before" and now >= target:
            raise SkipRunTimeWarning(f"current time {now} is after {target}")
        if when == "after" and now <= target:
            raise SkipRunTimeWarning(f"current time {now} is before {target}")
