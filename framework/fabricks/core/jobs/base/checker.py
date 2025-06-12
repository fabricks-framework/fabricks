from typing import Literal

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.error import (
    PostRunCheckException,
    PostRunCheckWarning,
    PreRunCheckException,
    PreRunCheckWarning,
    SkipRunCheckWarning,
)
from fabricks.core.jobs.base.generator import Generator


class Checker(Generator):
    def check_pre_run(self):
        self._check("pre_run")

    def check_post_run(self):
        self._check("post_run")

    def _check(self, position: Literal["pre_run", "post_run"]):
        if self.options.check.get(position):
            DEFAULT_LOGGER.debug(f"{position.replace('_', ' ')} check", extra={"job": self})

            p = self.paths.runtime.append(f".{position}.sql")
            assert p.exists(), f"{position} check not found ({p})"

            df = self.spark.sql(p.get_sql())
            fail_df = df.where("__action == 'fail'")
            warning_df = df.where("__action == 'warning'")

            if not fail_df.isEmpty():
                for row in fail_df.collect():
                    DEFAULT_LOGGER.error(
                        f"{position.replace('_', ' ')} check failed due to {row['__message']}",
                        extra={"job": self},
                    )

                if position == "pre_run":
                    raise PreRunCheckException(row["__message"], dataframe=df)
                elif position == "post_run":
                    raise PostRunCheckException(row["__message"], dataframe=df)

            elif not warning_df.isEmpty():
                for row in warning_df.collect():
                    DEFAULT_LOGGER.warning(
                        f"{position.replace('_', ' ')} check failed due to {row['__message']}",
                        extra={"job": self},
                    )

                if position == "pre_run":
                    raise PreRunCheckWarning(row["__message"], dataframe=df)
                elif position == "post_run":
                    raise PostRunCheckWarning(row["__message"], dataframe=df)

    def check_post_run_extra(self):
        min_rows = self.options.check.get("min_rows")
        max_rows = self.options.check.get("max_rows")
        count_must_equal = self.options.check.get("count_must_equal")

        if min_rows or max_rows or count_must_equal:
            DEFAULT_LOGGER.debug("extra post run check", extra={"job": self})

            df = self.spark.sql(f"select count(*) from {self}")
            rows = df.collect()[0][0]
            if min_rows:
                if rows < min_rows:
                    raise PostRunCheckException(f"min rows check failed ({rows} < {min_rows})", dataframe=df)

            if max_rows:
                if rows > max_rows:
                    raise PostRunCheckException(f"max rows check failed ({rows} > {max_rows})", dataframe=df)

            if count_must_equal:
                equals_rows = self.spark.read.table(count_must_equal).count()
                if rows != equals_rows:
                    raise PostRunCheckException(
                        f"count must equal check failed ({count_must_equal} - {rows} != {equals_rows})",
                        dataframe=df,
                    )

    def _check_duplicate_in_column(self, column: str):
        if column in self.table.columns:
            DEFAULT_LOGGER.debug(f"duplicate {column} check", extra={"job": self})

            cols = [column]

            if "__source" in self.table.columns:
                cols.append("__source")

            if self.change_data_capture == "scd2":
                cols.append("__valid_to")
            elif self.change_data_capture == "nocdc":
                if "__valid_to" in self.table.columns:
                    cols.append("__valid_to")

            cols = ", ".join(cols)
            df = self.spark.sql(f"select {cols} from {self} group by all having count(*) > 1 limit 5")

            if not df.isEmpty():
                duplicates = ",".join([str(row[column]) for row in df.collect()])
                raise PostRunCheckException(
                    f"duplicate {column} check failed ({duplicates})",
                    dataframe=df,
                )

        else:
            DEFAULT_LOGGER.debug(f"{column} not found", extra={"job": self})

    def check_duplicate_key(self):
        self._check_duplicate_in_column("__key")

    def check_duplicate_hash(self):
        self._check_duplicate_in_column("__hash")

    def check_duplicate_identity(self):
        self._check_duplicate_in_column("__identity")

    def check_skip_run(self):
        if self.options.check.get("skip"):
            DEFAULT_LOGGER.debug("skip check", extra={"job": self})

            p = self.paths.runtime.append(".skip.sql")
            assert p.exists(), "skip check not found"

            df = self.spark.sql(p.get_sql())
            skip_df = df.where("__skip")
            if not skip_df.isEmpty():
                for row in skip_df.collect():
                    DEFAULT_LOGGER.warning(
                        f"skip run due to {row['__message']}",
                        extra={"job": self},
                    )

                raise SkipRunCheckWarning(row["__message"], dataframe=df)
