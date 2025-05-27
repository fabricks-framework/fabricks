from abc import abstractmethod
from functools import partial
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import IS_UNITY_CATALOG, SECRET_SCOPE
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core.jobs.base.error import (
    PostRunCheckException,
    PostRunCheckWarning,
    PostRunInvokeException,
    PreRunCheckException,
    PreRunCheckWarning,
    PreRunInvokeException,
    SkipRunCheckWarning,
)
from fabricks.core.jobs.base.invoker import Invoker
from fabricks.utils.write import write_stream


class Processor(Invoker):
    def filter_where(self, df: DataFrame) -> DataFrame:
        f = self.options.job.get("filter_where")

        if f:
            DEFAULT_LOGGER.debug(f"filter where {f}", extra={"job": self})
            df = df.where(f"{f}")

        return df

    def encrypt(self, df: DataFrame) -> DataFrame:
        encrypted_columns = self.options.job.get_list("encrypted_columns")
        if encrypted_columns:
            if not IS_UNITY_CATALOG:
                from databricks.sdk.runtime import dbutils

                key = dbutils.secrets.get(scope=SECRET_SCOPE, key="encryption-key")
            else:
                import os

                key = os.environ["FABRICKS_ENCRYPTION_KEY"]

            assert key, "key not found"

            for col in encrypted_columns:
                DEFAULT_LOGGER.debug(f"encrypt column: {col}", extra={"job": self})
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

    def _for_each_batch(self, df: DataFrame, batch: Optional[int] = None, **kwargs):
        DEFAULT_LOGGER.debug("for each batch starts", extra={"job": self})
        if batch is not None:
            DEFAULT_LOGGER.debug(f"batch {batch}", extra={"job": self})

        df = self.base_transform(df)

        if self.schema_drifted(df):
            if self.schema_drift or kwargs.get("reload", False):
                DEFAULT_LOGGER.warning("schema drifted", extra={"job": self})
                self.update_schema(df=df)

            else:
                raise ValueError("schema drifted")

        self.for_each_batch(df, batch, **kwargs)

        if batch is not None:
            self.table.set_property("fabricks.last_batch", batch)

        self.table.create_restore_point()
        DEFAULT_LOGGER.debug("for each batch ends", extra={"job": self})

    def for_each_run(self, **kwargs):
        DEFAULT_LOGGER.debug("for each run starts", extra={"job": self})

        if self.virtual:
            self.create_or_replace_view()

        elif self.persist:
            assert self.table.exists(), "delta table not found"

            df = self.get_data(self.stream)
            assert df is not None, "no data"

            partial(self._for_each_batch, **kwargs)

            if self.stream:
                DEFAULT_LOGGER.debug("stream enabled", extra={"job": self})
                write_stream(
                    df,
                    checkpoints_path=self.paths.checkpoints,
                    func=self._for_each_batch,
                    timeout=self.timeout,
                )
            else:
                self._for_each_batch(df, **kwargs)

        else:
            raise ValueError(f"{self.mode} - not allowed")

        DEFAULT_LOGGER.debug("for each run ends", extra={"job": self})

    def run(
        self,
        retry: Optional[bool] = True,
        schedule: Optional[str] = None,
        schedule_id: Optional[str] = None,
        invoke: Optional[bool] = True,
        reload: Optional[bool] = None,
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
                DEFAULT_LOGGER.debug(f"last version {last_version}", extra={"job": self})
            else:
                last_version = str(self.table.last_version)

            last_batch = self.table.get_property("fabricks.last_batch")
            if last_batch is not None:
                DEFAULT_LOGGER.debug(f"last batch {last_batch}", extra={"job": self})

        try:
            DEFAULT_LOGGER.info("run starts", extra={"job": self})
            if reload:
                DEFAULT_LOGGER.debug("force reload", extra={"job": self})

            if invoke:
                self.invoke_pre_run(schedule=schedule)

            if not reload:
                self.check_skip_run()

            self.check_pre_run()

            self.for_each_run(schedule=schedule, reload=reload)

            self.check_post_run()
            self.check_post_run_extra()

            if invoke:
                self.invoke_post_run(schedule=schedule)

            DEFAULT_LOGGER.info("run ends", extra={"job": self})

        except SkipRunCheckWarning as e:
            DEFAULT_LOGGER.warning("skip run", extra={"job": self})
            raise e

        except (PreRunCheckWarning, PostRunCheckWarning) as e:
            DEFAULT_LOGGER.warning("could not pass warning check", extra={"job": self})
            raise e

        except (PreRunInvokeException, PostRunInvokeException) as e:
            DEFAULT_LOGGER.exception("could not run invoker", extra={"job": self})
            raise e

        except (PreRunCheckException, PostRunCheckException) as e:
            DEFAULT_LOGGER.exception("could not pass check", extra={"job": self})
            self.restore(last_version, last_batch)
            raise e

        except AssertionError as e:
            DEFAULT_LOGGER.exception("could not run", extra={"job": self})
            self.restore(last_version, last_batch)
            raise e

        except Exception as e:
            if not self.stream or not retry:
                DEFAULT_LOGGER.exception("could not run", extra={"job": self})
                self.restore(last_version, last_batch)
                raise e

            else:
                DEFAULT_LOGGER.warning("retry to run", extra={"job": self})
                self.run(retry=False, schedule_id=schedule_id)

    @abstractmethod
    def overwrite(self):
        raise NotImplementedError()
