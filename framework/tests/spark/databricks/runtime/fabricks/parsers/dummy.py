from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from fabricks.core.parsers import BaseParser, parser
from fabricks.models import ParserOptions
from fabricks.utils.path import FileSharePath


@parser(name="dummy")
class DummyParser(BaseParser):
    """Proves get_parser() loads a custom parser plugin from PATH_PARSERS -- otherwise
    identical to the built-in json parser, so bronze.king_scd1 stays a plain streaming
    file-parse job with no delete-log handling."""

    def __init__(self, options: ParserOptions | None = None):
        super().__init__(options, "json")

    def parse(
        self, data_path: FileSharePath, schema_path: FileSharePath, spark: SparkSession, stream: bool
    ) -> DataFrame:
        df = super().parse(data_path=data_path, schema_path=schema_path, spark=spark, stream=stream)
        return df.withColumn("__parsed_by", lit("dummy"))
