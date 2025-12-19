from pyspark.sql import DataFrame, SparkSession
from runtime.fabricks.parsers.delete_log import DeleteLogBaseParser

from fabricks.core.parsers import parser
from fabricks.utils.path import Path


@parser(name="monarch")
class MonarchParser(DeleteLogBaseParser):
    def parse(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> DataFrame:
        df = super().parse(stream=stream, data_path=data_path, schema_path=schema_path, spark=spark)
        if df:
            cols = [c for c in df.columns if c.startswith("BEL_")]
            if cols:
                df = df.drop(*cols)
        return df
