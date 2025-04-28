from typing import Optional, Union

from pyspark.sql import DataFrame, SparkSession

from fabricks.cdc.base import BaseCDC
from fabricks.metastore.table import Table


class NoCDC(BaseCDC):
    def __init__(
        self,
        database: str,
        *levels: str,
        spark: Optional[SparkSession] = None,
    ):
        super().__init__(database, *levels, change_data_capture="nocdc", spark=spark)

    def complete(self, src: Union[DataFrame, Table, str], **kwargs):
        self.overwrite(src=src, **kwargs)
