from typing import Optional

from pyspark.sql import SparkSession

from fabricks.cdc.scd import SCD
from fabricks.models.cdc import CdcContext


class NoCDC(SCD):
    def __init__(
        self,
        database: str,
        *levels: str,
        spark: Optional[SparkSession] = None,
    ):
        super().__init__(database, *levels, change_data_capture="nocdc", spark=spark)

    def delete_missing(self, src, context: CdcContext):
        self.merge(src, context.model_copy(update={"delete_missing": True, "mode": "update"}))
