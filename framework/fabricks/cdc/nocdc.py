from typing import Optional

from pyspark.sql import SparkSession

from fabricks.cdc.scd import SCD


class NoCDC(SCD):
    def __init__(
        self,
        database: str,
        *levels: str,
        spark: Optional[SparkSession] = None,
    ):
        super().__init__(database, *levels, change_data_capture="nocdc", spark=spark)

    def delete_missing(self, src, **kwargs):
        kwargs["delete_missing"] = True
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)
