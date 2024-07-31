from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from fabricks.core.parsers import parser
from fabricks.utils.path import Path
from tests.runtime.fabricks.parsers.delete_log import DeleteLogBaseParser


@parser(name="monarch")
class MonarchParser(DeleteLogBaseParser):
    def parse(self, stream: bool, data_path: Path, schema_path: Path, spark: SparkSession) -> Optional[DataFrame]:
        df = super().parse(stream=stream, data_path=data_path, schema_path=schema_path, spark=spark)
        if df:
            cols = [c for c in df.columns if c.startswith("BEL_")]
            if cols:
                df = df.drop(*cols)
        return df
