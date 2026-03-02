from typing import Optional

from pyspark.sql import SparkSession

from fabricks.cdc.scd import SCD


class SCD0(SCD):
    def __init__(
        self,
        database: str,
        *levels: str,
        spark: Optional[SparkSession] = None,
    ):
        super().__init__(database, *levels, change_data_capture="scd0", spark=spark)
