from typing import Any

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.scd import SCD
from fabricks.metastore.table import Table


class NoCDC(SCD):
    def __init__(self, database: str, *levels: str, spark: SparkSession | None = None) -> None:
        super().__init__(database, *levels, change_data_capture="nocdc", spark=spark)

    def delete_missing(self, src: DataFrame | Table | str, **kwargs: Any) -> None:  # noqa: ANN401 - heterogeneous options bag forwarded through the cdc query pipeline
        kwargs["delete_missing"] = True
        kwargs["mode"] = "update"
        self.merge(src, **kwargs)
