from __future__ import annotations

import datetime
from typing import Literal

from pyspark.sql import DataFrame

from fabricks.context import TIMEZONE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.protocols import CheckableJob


class CheckException(Exception):
    def __init__(self, message: str, dataframe: DataFrame | None = None):
        self.message = message
        self.dataframe = dataframe
        super().__init__(self.message)


class CheckWarning(CheckException):
    pass


class PreRunCheckException(CheckException):
    pass


class PostRunCheckException(CheckException):
    pass


class PreRunCheckWarning(CheckWarning):
    pass


class PostRunCheckWarning(CheckWarning):
    pass


class SkipWarning(CheckException):
    pass


class SkipRunCheckWarning(SkipWarning):
    pass


class SkipRunTimeWarning(SkipWarning):
    pass


class JobChecker:
    def __init__(self, job: CheckableJob):
        self._job = job

    def check_pre_run(self):
        self._check("pre_run")

    def check_post_run(self):
        self._check("post_run")

    def _check(self, position: Literal["pre_run", "post_run"]):
        if self._job.check_options and getattr(self._job.check_options, position):
            DEFAULT_LOGGER.debug(f"check {position}", extra={"label": self._job})

            p = self._job.paths.to_runtime.append(f".{position}.sql")
            assert p.exists(), f"{position} check not found ({p})"

            df = self._job.spark.sql(p.get_sql())
            fail_df = df.where("__action == 'fail'")
            warning_df = df.where("__action == 'warning'")

            rows = fail_df.collect()
            if rows:
                for row in rows:
                    DEFAULT_LOGGER.warning(
                        f"check {position} failed due to {row['__message']}",
                        extra={"label": self._job},
                    )

                if position == "pre_run":
                    raise PreRunCheckException(rows[-1]["__message"], dataframe=df)
                elif position == "post_run":
                    raise PostRunCheckException(rows[-1]["__message"], dataframe=df)

            rows = warning_df.collect()
            if rows:
                for row in rows:
                    DEFAULT_LOGGER.warning(
                        f"check {position} failed due to {row['__message']}",
                        extra={"label": self._job},
                    )

                if position == "pre_run":
                    raise PreRunCheckWarning(rows[-1]["__message"], dataframe=df)
                elif position == "post_run":
                    raise PostRunCheckWarning(rows[-1]["__message"], dataframe=df)

    def check_post_run_extra(self):
        check_options = self._job.check_options
        min_rows = check_options.min_rows if check_options else None
        max_rows = check_options.max_rows if check_options else None
        count_must_equal = check_options.count_must_equal if check_options else None

        if min_rows or max_rows or count_must_equal:
            df = self._job.spark.sql(f"select count(*) from {self._job}")
            rows = df.collect()[0][0]
            if min_rows:
                DEFAULT_LOGGER.debug("check min rows", extra={"label": self._job})
                if rows < min_rows:
                    raise PostRunCheckException(f"min rows check failed ({rows} < {min_rows})", dataframe=df)

            if max_rows:
                DEFAULT_LOGGER.debug("check max rows", extra={"label": self._job})
                if rows > max_rows:
                    raise PostRunCheckException(f"max rows check failed ({rows} > {max_rows})", dataframe=df)

            if count_must_equal:
                DEFAULT_LOGGER.debug("check count must equal", extra={"label": self._job})
                equals_rows = self._job.spark.read.table(count_must_equal).count()
                if rows != equals_rows:
                    raise PostRunCheckException(
                        f"count must equal check failed ({count_must_equal} - {rows} != {equals_rows})",
                        dataframe=df,
                    )

    def _check_duplicate_in_column(self, column: str):
        if column in self._job.table.columns:
            DEFAULT_LOGGER.debug(f"check duplicate in {column}", extra={"label": self._job})

            cols = [column]

            if "__source" in self._job.table.columns:
                cols.append("__source")

            if self._job.change_data_capture == "scd2":
                cols.append("__valid_to")

            elif self._job.change_data_capture == "nocdc":
                if "__valid_to" in self._job.table.columns:
                    cols.append("__valid_to")
                elif self._job.mode == "append" and "__timestamp" in self._job.table.columns:
                    cols.append("__timestamp")

            cols_str = ", ".join(cols)
            df = self._job.spark.sql(f"select {cols_str} from {self._job} group by all having count(*) > 1 limit 5")

            duplicate_rows = df.collect()
            if duplicate_rows:
                duplicates = ",".join([str(row[column]) for row in duplicate_rows])
                raise PostRunCheckException(
                    f"duplicate {column} check failed ({duplicates})",
                    dataframe=df,
                )

        else:
            DEFAULT_LOGGER.debug(f"could not find {column}", extra={"label": self._job})

    def check_duplicate_key(self):
        self._check_duplicate_in_column("__key")

    def check_duplicate_hash(self):
        self._check_duplicate_in_column("__hash")

    def check_duplicate_identity(self):
        self._check_duplicate_in_column("__identity")

    def check_skip_run(self):
        if self._job.check_options and self._job.check_options.skip:
            DEFAULT_LOGGER.debug("check if run should be skipped", extra={"label": self._job})

            p = self._job.paths.to_runtime.append(".skip.sql")
            assert p.exists(), "skip check not found"

            df = self._job.spark.sql(p.get_sql())
            skip_df = df.where("__skip")

            skip_rows = skip_df.collect()
            if skip_rows:
                for row in skip_rows:
                    DEFAULT_LOGGER.warning(
                        f"skip run due to {row['__message']}",
                        extra={"label": self._job},
                    )

                raise SkipRunCheckWarning(skip_rows[-1]["__message"], dataframe=df)

    def check_run_before(self):
        if self._job.check_options and self._job.check_options.before:
            self._check_run_time(self._job.check_options.before, "before")

    def check_run_after(self):
        if self._job.check_options and self._job.check_options.after:
            self._check_run_time(self._job.check_options.after, "after")

    def _check_run_time(self, time: str, when: Literal["before", "after"]):
        now = datetime.datetime.now(tz=TIMEZONE)
        time_as_time = datetime.datetime.strptime(time, "%H:%M:%S").time()
        target = datetime.datetime.combine(now.date(), time_as_time, tzinfo=TIMEZONE)

        DEFAULT_LOGGER.debug(f"check {when} {target}", extra={"label": self._job})

        if when == "before" and now >= target:
            raise SkipRunTimeWarning(f"current time {now} is after {target}")
        elif when == "after" and now <= target:
            raise SkipRunTimeWarning(f"current time {now} is before {target}")


# Backward-compatible name
Checker = JobChecker
