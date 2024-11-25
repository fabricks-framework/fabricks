import re
from typing import Optional, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import FABRICKS_STORAGE, SECRET_SCOPE, SPARK
from fabricks.core.dags.log import DagsTableLogger
from fabricks.metastore.table import Table
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.secret import AccessKey, get_secret_from_secret_scope


class BaseDags:
    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id
        self._connection_string = None
        self._table = None

    def get_connection_string(self) -> str:
        if not self._connection_string:
            storage_account = FABRICKS_STORAGE.get_storage_account()
            secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
            access_key = cast(AccessKey, secret).key
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={access_key};EndpointSuffix=core.windows.net"
            self._connection_string = connection_string
        return self._connection_string

    def get_table(self) -> AzureTable:
        if not self._table:
            cs = self.get_connection_string()
            self._table = AzureTable(f"t{self.schedule_id}", connection_string=cs)
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

        d = DagsTableLogger.table.query(q)
        df = SPARK.createDataFrame(d)
        if "Exception" not in df.columns:
            df = df.withColumn("Exception", expr("null"))

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
            .save(Table("fabricks", "logs").deltapath.string)
        )

    def remove_invalid_characters(self, s: str) -> str:
        out = re.sub("[^a-zA-Z0-9]", "", s)
        return out
