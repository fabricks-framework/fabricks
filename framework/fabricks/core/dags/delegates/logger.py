from __future__ import annotations

import re
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.context import SPARK
from fabricks.core.dags.log import TABLE_LOG_HANDLER
from fabricks.core.dags.protocols import BaseDagsProtocol
from fabricks.metastore.table import Table


class DagLogger:
    def __init__(self, dags: BaseDagsProtocol):
        self._dags = dags

    def get_logs(self, step: Optional[str] = None) -> DataFrame:
        schedule_id = self._dags.schedule_id
        q = f"PartitionKey eq '{schedule_id}'"
        if step:
            q += f" and Step eq '{step}'"

        d = TABLE_LOG_HANDLER.table.query(q)
        df = SPARK.createDataFrame(d)

        for column in ["Exception", "NotebookId", "Json"]:
            if column not in df.columns:
                df = df.withColumn(column, expr("null"))

        return SPARK.sql(
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
              from_json(Exception, 'type STRING, message STRING, traceback STRING') as exception,
              Json as json
            from
              {df}
            """,
            df=df,
        )

    def write_logs(self, df: DataFrame):
        try:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .option("partitionOverwriteMode", "dynamic")
                .save(Table("fabricks", "logs").delta_path.string)
            )
        except Exception:
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("partitionOverwriteMode", "dynamic")
                .save(Table("fabricks", "logs").delta_path.string)
            )

    def remove_invalid_characters(self, s: str) -> str:
        return re.sub("[^a-zA-Z0-9]", "", s)
