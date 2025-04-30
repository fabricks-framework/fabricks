from typing import Optional

from py4j.protocol import Py4JError
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, lit, when

from fabricks.core.parsers import BaseParser, ParserOptions
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.path import Path
from fabricks.utils.read import read


class DeleteLogBaseParser(BaseParser):
    def __init__(self, options: Optional[ParserOptions] = None):
        if options:
            file_format = options.get("file_format") or "parquet"
        else:
            file_format = "parquet"
        super().__init__(options, file_format)

    def _parse(
        self,
        stream: bool,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
    ) -> Optional[DataFrame]:
        df = read(
            stream=stream,
            path=data_path,
            file_format=self.file_format,
            schema_path=schema_path,
            options=self.options.get("read_options"),
            spark=spark,
        )

        cols = [c.casefold() for c in df.columns]
        if "BEL_IsFullLoad".casefold() in cols:
            df = df.withColumn(
                "__operation",
                expr("if(BEL_IsFullLoad == 1, 'reload', 'upsert')"),
            )
        else:
            df = df.withColumn("__operation", lit("upsert"))

        return df

    def _parse_delete_log(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> Optional[DataFrame]:
        data_path = data_path.append("__deletelog")
        schema_path = schema_path.append("__deletelog")
        try:
            df = read(
                stream=stream,
                path=data_path,
                file_format=self.file_format,
                schema_path=schema_path,
                options=self.options.get("read_options"),
                spark=spark,
            )
            df.columns
            return df.withColumn("__operation", lit("delete"))

        except (AnalysisException, Py4JError, SparkConnectGrpcException):
            if stream:
                df = spark.readStream.table("fabricks.dummy")
                df = df.selectExpr("'delete' as __operation").where("1 == 2")
                return df.withColumn("__operation", lit("delete"))

    def nullify(self, df: DataFrame) -> DataFrame:
        if df:
            for c in [c for c in df.columns if not c.startswith("__")]:
                df = df.withColumn(
                    c,
                    when(df[f"`{c}`"].cast("string") == "1753-01-01 00:00:00.000", None).otherwise(df[f"`{c}`"]),
                )
        return df

    def parse(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> DataFrame:
        dfs = []

        df = self._parse(stream=stream, data_path=data_path, schema_path=schema_path, spark=spark)
        dfs.append(df)

        df_del = self._parse_delete_log(stream=stream, data_path=data_path, schema_path=schema_path, spark=spark)
        dfs.append(df_del)

        df = concat_dfs(dfs)
        assert df is not None

        df = self.add_timestamp_from_file_path(df)
        df = self.nullify(df)
        # avoid fake updates based on the BEL_UpdateDateUtc
        df = df.drop("BEL_IsFullLoad", "BEL_UpdateDateUtc", "BEL_DeleteDateUtc")
        return df
