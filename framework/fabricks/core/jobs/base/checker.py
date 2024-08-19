from fabricks.context.log import Logger
from fabricks.core.jobs.base.error import CheckFailedException, CheckWarningException
from fabricks.core.jobs.base.generator import Generator


class Checker(Generator):
    def pre_run_check(self):
        self._check("pre_run")

    def post_run_check(self):
        self._check("post_run")

    def _check(self, position: str):
        if self.options.check.get(position):
            Logger.debug(f"{position.replace('_', ' ')} check", extra={"job": self})

            p = self.paths.runtime.append(f".{position}.sql")
            assert p.exists(), f"{position} check not found ({p})"

            fail_df = self.spark.sql(p.get_sql()).where("__action == 'fail'")
            warning_df = self.spark.sql(p.get_sql()).where("__action == 'warning'")

            if not fail_df.isEmpty():
                for row in fail_df.collect():
                    Logger.error(
                        f"{position.replace('_', ' ')} check failed due to {row['__message']}",
                        extra={"job": self},
                    )
                raise CheckFailedException(row["__message"])  # type: ignore
            elif not warning_df.isEmpty():
                for row in warning_df.collect():
                    Logger.warning(
                        f"{position.replace('_', ' ')} check failed due to {row['__message']}",
                        extra={"job": self},
                    )
                raise CheckWarningException(row["__message"])  # type: ignore

    def post_run_extra_check(self):
        min_rows = self.options.check.get("min_rows")
        max_rows = self.options.check.get("max_rows")
        count_must_equal = self.options.check.get("count_must_equal")

        if min_rows or max_rows or count_must_equal:
            Logger.debug("extra post run check", extra={"job": self})

            rows = self.spark.sql(f"select count(*) from {self}").collect()[0][0]
            if min_rows:
                if rows < min_rows:
                    raise CheckFailedException(f"min rows check failed ({rows} < {min_rows})")
            if max_rows:
                if rows > max_rows:
                    raise CheckFailedException(f"max rows check failed ({rows} > {max_rows})")

            if count_must_equal:
                equals_rows = self.spark.read.table(count_must_equal).count()
                if rows != equals_rows:
                    raise CheckFailedException(
                        f"count must equal check failed ({count_must_equal} - {rows} != {equals_rows})"
                    )

    def _check_duplicate(self, column: str):
        if column in self.table.columns:
            Logger.debug(f"duplicate {column} check", extra={"job": self})

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
                raise CheckFailedException(f"duplicate {column} check failed ({duplicates})")
        else:
            Logger.debug(f"{column} not found", extra={"job": self})

    def check_duplicate_key(self):
        self._check_duplicate("__key")

    def check_duplicate_hash(self):
        self._check_duplicate("__hash")
