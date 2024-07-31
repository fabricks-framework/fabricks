import re
from typing import Optional, cast

from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import FABRICKS_STORAGE
from fabricks.core.dags.log import DagsTableLogger
from fabricks.metastore.table import Table
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.secret import AccessKey, get_secret_from_secret_scope


class BaseDags:
    def __init__(self, schedule_id: str):
        self.schedule_id = schedule_id

    def get_connection_string(self) -> str:
        storage_account = FABRICKS_STORAGE.get_storage_account()
        secret = get_secret_from_secret_scope("bmskv", f"{storage_account}-access-key")
        access_key = cast(AccessKey, secret).key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={access_key};EndpointSuffix=core.windows.net"
        return connection_string

    def get_table(self) -> AzureTable:
        cs = self.get_connection_string()
        table = AzureTable(f"t{self.schedule_id}", connection_string=cs)
        return table

    def get_logs(self, step: Optional[str] = None) -> DataFrame:
        q = f"PartitionKey eq '{self.schedule_id}'"
        if step:
            q += f" and Step eq '{step}'"

        d = DagsTableLogger.table.query(q)
        df = spark.createDataFrame(d)
        if "Exception" not in df.columns:
            df = df.withColumn("Exception", expr("null"))

        df = spark.sql(
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
