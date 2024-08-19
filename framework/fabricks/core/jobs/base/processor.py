from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import SECRET_SCOPE
from fabricks.context.log import Logger, flush
from fabricks.core.jobs.base.error import CheckFailedException, CheckWarningException, InvokerFailedException
from fabricks.core.jobs.base.invoker import Invoker
from fabricks.utils.write import write_stream


class Processor(Invoker):
    def extender(self, df: DataFrame) -> DataFrame:
        name = self.options.job.get("extender")
        if not name:
            name = self.step_conf.get("options", {}).get("extender", None)

        if name:
            from fabricks.core.extenders import get_extender

            Logger.debug(f"extend ({name})", extra={"job": self})
            df = df.transform(get_extender(name))
        return df

    def filter_where(self, df: DataFrame) -> DataFrame:
        f = self.options.job.get("filter_where")
        if f:
            Logger.debug(f"filter where {f}", extra={"job": self})
            df = df.where(f"{f}")
        return df

    def encrypt(self, df: DataFrame) -> DataFrame:
        encrypted_columns = self.options.job.get_list("encrypted_columns")
        if encrypted_columns:
            key = self.dbutils.secrets.get(scope=SECRET_SCOPE, key="encryption-key")
            assert key, "key not found"
            for col in encrypted_columns:
                Logger.debug(f"encrypt column: {col}", extra={"job": self})
                df = df.withColumn(col, expr(f"aes_encrypt({col}, '{key}')"))
        return df

    def restore(self, last_version: Optional[str] = None, last_batch: Optional[str] = None):
        """
        Restores the processor to a specific version and batch.

        Args:
            last_version (Optional[str]): The last version to restore to. If None, no version restore will be performed.
            last_batch (Optional[str]): The last batch to restore to. If None, no batch restore will be performed.
        """
        if self.persist:
            if last_version is not None:
                _last_version = int(last_version)
                if self.table.get_last_version() > _last_version:
                    self.table.restore_to_version(_last_version)

            if last_batch is not None:
                current_batch = int(last_batch) + 1
                self.rm_commit(current_batch)
                assert last_batch == self.table.get_property("fabricks.last_batch")
                assert self.paths.commits.join(last_batch).exists()

    def _for_each_batch(self, df: DataFrame, batch: Optional[int] = None):
        Logger.debug("for each batch starts", extra={"job": self})
        if batch is not None:
            Logger.debug(f"batch {batch}", extra={"job": self})

        df = self.base_transform(df)

        drift = self.table.schema_drifted(df)
        if drift:
            if self.schema_drift:
                Logger.warning("schema drifted", extra={"job": self})
                self.update_schema(df=df)
            else:
                raise ValueError("schema drifted")

        self.for_each_batch(df, batch)

        if batch is not None:
            self.table.set_property("fabricks.last_batch", batch)

        self.table.create_restore_point()
        Logger.debug("for each batch ends", extra={"job": self})

    def for_each_run(self, schedule: Optional[str] = None):
        Logger.debug("for each run starts", extra={"job": self})

        if self.virtual:
            if self.schema_drift:
                self.create_or_replace_view()

        elif self.persist:
            assert self.table.exists(), "delta table not found"

            df = self.get_data(self.stream)
            assert df is not None, "no data"

            if self.stream:
                Logger.debug("stream enabled", extra={"job": self})
                write_stream(
                    df,
                    checkpoints_path=self.paths.checkpoints,
                    func=self._for_each_batch,
                    timeout=self.timeouts.job,
                )
            else:
                self._for_each_batch(df)

        else:
            raise ValueError(f"{self.mode} - not allowed")

        Logger.debug("for each run ends", extra={"job": self})

    @flush
    def run(
        self,
        retry: Optional[bool] = True,
        schedule: Optional[str] = None,
        schedule_id: Optional[str] = None,
        invoke: Optional[bool] = True,
    ):
        """
        Run the processor.

        Args:
            retry (bool, optional): Whether to retry the execution in case of failure. Defaults to True.
            schedule (str, optional): The schedule to run the processor on. Defaults to None.
            schedule_id (str, optional): The ID of the schedule. Defaults to None.
            invoke (bool, optional): Whether to invoke pre-run and post-run methods. Defaults to True.
        """
        last_version = None
        last_batch = None

        if self.persist:
            last_version = self.table.get_property("fabricks.last_version")
            if last_version is not None:
                Logger.debug(f"last version {last_version}", extra={"job": self})
            else:
                last_version = str(self.table.last_version)

            last_batch = self.table.get_property("fabricks.last_batch")
            if last_batch is not None:
                Logger.debug(f"last batch {last_batch}", extra={"job": self})

        try:
            Logger.info("run starts", extra={"job": self})

            if invoke:
                self.pre_run_invoke(schedule=schedule)

            self.pre_run_check()

            self.for_each_run(schedule=schedule)

            self.post_run_check()
            self.post_run_extra_check()

            if invoke:
                self.post_run_invoke(schedule=schedule)

            Logger.info("run ends", extra={"job": self})

        except CheckWarningException as e:
            Logger.exception("🙈 (no retry)", extra={"job": self})
            raise e
        except InvokerFailedException as e:
            Logger.exception("🙈 (no retry)", extra={"job": self})
            raise e
        except CheckFailedException as e:
            Logger.exception("🙈 (no retry)", extra={"job": self})
            self.restore(last_version, last_batch)
            raise e
        except AssertionError as e:
            Logger.exception("🙈", extra={"job": self})
            self.restore(last_version, last_batch)
            raise e
        except Exception as e:
            if not self.stream or not retry:
                Logger.exception("🙈 (no retry)", extra={"job": self})
                self.restore(last_version, last_batch)
                raise e
            else:
                Logger.exception("🙈 (retry)", extra={"job": self})
                self.run(retry=False, schedule_id=schedule_id)

    def overwrite(self):
        """
        Executes the overwrite job.

        This method truncates the data, overwrites the schema, and runs the job.
        If an exception occurs during the execution, it is logged and re-raised.

        Raises:
            Exception: If an error occurs during the execution of the job.
        """
        try:
            Logger.warning("overwrite job", extra={"job": self})
            self.truncate()
            self.overwrite_schema()
            self.run(retry=False)
        except Exception as e:
            Logger.exception("🙈", extra={"job": self})
            raise e
