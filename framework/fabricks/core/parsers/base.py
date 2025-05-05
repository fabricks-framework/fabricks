from abc import ABC
from typing import Callable, Optional, final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, from_json, lit
from pyspark.sql.types import MapType, StringType

from fabricks.core.parsers._types import ParserOptions
from fabricks.core.utils import clean
from fabricks.utils.path import Path
from fabricks.utils.read.read import read


class BaseParser(ABC):
    def __init__(self, options: Optional[ParserOptions], file_format: str):
        self.options = options or {}
        self.file_format = file_format

    def add_timestamp_from_file_path(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "__split",
            expr("split(replace(__metadata.file_path, __metadata.file_name), '/')"),
        )
        df = df.withColumn("__split_size", expr("size(__split)"))
        df = df.withColumn(
            "__timestamp",
            expr("left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)"),
        )
        df = df.withColumn("__timestamp", expr("to_timestamp(__timestamp, 'yyyyMMddHHmmss')"))
        df = df.drop("__split", "__split_size")

        return df

    def parse(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> DataFrame:
        df = read(
            stream=stream,
            path=data_path,
            file_format=self.file_format,
            schema_path=schema_path,
            options=self.options.get("read_options"),
            spark=spark,
        )

        if "__timestamp" not in df.columns:
            df = self.add_timestamp_from_file_path(df)

        return df

    @final
    def get_data(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> DataFrame:
        """
        Retrieves and processes data from the specified data path using the provided schema.

        Args:
            data_path (Path): The path to the data file.
            schema_path (Path): The path to the schema file.
            spark (SparkSession): The SparkSession object.
            stream (bool): Indicates whether the data should be processed as a stream.

        Returns:
            DataFrame: The processed data as a DataFrame.

        Raises:
            AssertionError: If the "__timestamp" column is missing in the DataFrame.
            AssertionError: If the "__metadata.file_path" column is missing in the DataFrame.
        """
        df = self.parse(data_path=data_path, schema_path=schema_path, spark=spark, stream=stream)
        df = df.transform(clean)

        if "__rescued_data" not in df.columns:
            df = df.withColumn("__rescued_data", lit(None).cast(StringType()))

        df = df.withColumn("__rescued_data", from_json(col("__rescued_data"), MapType(StringType(), StringType())))  # type: ignore

        assert "__timestamp" in df.columns, "__timestamp mandatory in dataframe"
        assert df.select("__metadata.file_path"), "file_path mandatory in struct __metadata in dataframe"
        return df

    def __str__(self):
        return f"{type(self).__name__} ({self.file_format})"


PARSERS: dict[str, Callable[[Optional[ParserOptions]], BaseParser]] = {}
