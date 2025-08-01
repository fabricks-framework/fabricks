import re
from typing import Optional, TypeAlias, TypedDict, cast, TYPE_CHECKING

from azure.core.exceptions import ServiceRequestError
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context import FABRICKS_STORAGE, SECRET_SCOPE, SPARK, IS_UNITY_CATALOG, FABRICKS_STORAGE_CREDENTIAL, DBUTILS
from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
from fabricks.core.dags.log import TABLE_LOG_HANDLER
from fabricks.metastore.table import Table
from fabricks.utils.azure_table import AzureTable
if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

class BaseDags:
    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id
        self._connection_info = None
        self._table = None

    def get_connection_info(self) -> dict:
        if not self._connection_info:
            storage_account = FABRICKS_STORAGE.get_storage_account()
            if not IS_UNITY_CATALOG:
                from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
                secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
                access_key = cast(AccessKey, secret).key
                credential : "TokenCredential | None" = None
                self._connection_info = {
                    "storage_account": storage_account,
                    "access_key": access_key,
                }
            else:
                import os
                
                access_key = os.environ.get("FABRICKS_ACCESS_KEY")
                credential : "TokenCredential | None" = DBUTILS.credentials.getServiceCredentialsProvider(FABRICKS_STORAGE_CREDENTIAL) if FABRICKS_STORAGE_CREDENTIAL else None # type: ignore
                if not access_key:
                    from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
                    secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
                    access_key = cast(AccessKey, secret).key
                assert credential or access_key, "path_options.storage_credential or FABRICKS_ACCESS_KEY must be set"
                self._connection_info = {
                    "storage_account": storage_account,
                    "access_key": access_key,
                    "credential": credential,
                }
        
        return self._connection_info

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, ServiceRequestError)),
        reraise=True,
    )
    def get_table(self) -> AzureTable:
        if not self._table:
            cs = self.get_connection_info()
            self._table = AzureTable(f"t{self.schedule_id}", **dict(cs)) # type: ignore

        if self._table is None:
            raise ValueError("Azure table for logs not found")

        return self._table

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self._table is not None:
            self._table.__exit__()

    def get_logs(self, step: Optional[str] = None) -> DataFrame:
        q = f"PartitionKey eq '{self.schedule_id}'"
        if step:
            q += f" and Step eq '{step}'"

        d = TABLE_LOG_HANDLER.table.query(q)
        df = SPARK.createDataFrame(d)

        if "Exception" not in df.columns:
            df = df.withColumn("Exception", expr("null"))
        if "NotebookId" not in df.columns:
            df = df.withColumn("NotebookId", expr("null"))

        df = SPARK.sql(
            """
            select
              ScheduleId as schedule_id,
              Schedule as schedule,
              Step as step,
              JobId as job_id,
              Job as job,
              NotebookId as notebook_id,
              `Level` as `level`,
              `Message` as `status`,
              to_timestamp(`Created`, 'dd/MM/yy HH:mm:ss') as `timestamp`,
              from_json(Exception, 'type STRING, message STRING, traceback STRING') as exception
            from
              {df}
            """,
            df=df,
        )

        return df

    def write_logs(self, df: DataFrame):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("partitionOverwriteMode", "dynamic")
            .save(Table("fabricks", "logs").delta_path.string)
        )

    def remove_invalid_characters(self, s: str) -> str:
        out = re.sub("[^a-zA-Z0-9]", "", s)
        return out
