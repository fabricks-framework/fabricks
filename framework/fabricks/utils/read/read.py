from typing import overload

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from fabricks.utils.path import FileSharePath


def _ensure_spark(spark: SparkSession | None) -> SparkSession:
    if spark is None:
        from fabricks.utils.spark import get_spark

        spark = get_spark()

    assert spark is not None
    return spark


@overload
def read_stream(
    src: FileSharePath | str,
    file_format: str,
    *,
    schema: StructType,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame: ...


@overload
def read_stream(
    src: FileSharePath | str,
    file_format: str,
    schema_path: FileSharePath | str,
    *,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame: ...


@overload
def read_stream(
    src: FileSharePath | str,
    file_format: str,
    *,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame: ...


def read_stream(
    src: FileSharePath | str,
    file_format: str,
    schema_path: FileSharePath | str | None = None,
    hints: str | list[str] | None = None,
    schema: StructType | None = None,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    return _read_stream(
        src=src,
        file_format=file_format,
        schema_path=schema_path,
        hints=hints,
        schema=schema,
        options=options,
        spark=spark,
    )


def _read_stream(
    src: FileSharePath | str,
    file_format: str,
    schema_path: FileSharePath | str | None = None,
    hints: str | list[str] | None = None,
    schema: StructType | None = None,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    spark = _ensure_spark(spark)

    if file_format == "table":
        assert isinstance(src, str)
        return spark.readStream.table(src)
    file_format = "binaryFile" if file_format == "pdf" else file_format

    if isinstance(src, str):
        src = FileSharePath(src)

    if file_format == "delta":
        reader = spark.readStream.format("delta")

    else:
        reader = spark.readStream.format("cloudFiles")
        reader.option("cloudFiles.format", file_format)

        if schema:
            reader.schema(schema)

        else:
            assert schema_path
            if isinstance(schema_path, str):
                schema_path = FileSharePath(schema_path)

            reader.option("cloudFiles.inferColumnTypes", "true")
            reader.option("cloudFiles.useIncrementalListing", "true")
            reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            reader.option("cloudFiles.schemaLocation", schema_path.string)

            if hints:
                if isinstance(hints, str):
                    hints = [hints]
                reader.option("cloudFiles.schemaHints", f"{' ,'.join(hints)}")

    # default options
    reader.option("recursiveFileLookup", "true")
    reader.option("skipChangeCommits", "true")
    reader.option("ignoreDeletes", "true")

    if file_format == "csv":
        reader.option("header", "true")

    # custom / override options
    if options:
        for key, value in options.items():
            reader.option(key, value)

    df = reader.load(src.string)
    return df.withColumnRenamed("_rescued_data", "__rescued_data")


@overload
def read_batch(
    src: FileSharePath | str,
    file_format: str,
    schema: StructType,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame: ...


@overload
def read_batch(
    src: FileSharePath | str,
    file_format: str,
    *,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame: ...


def read_batch(
    src: FileSharePath | str,
    file_format: str,
    schema: StructType | None = None,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    return _read_batch(src=src, file_format=file_format, schema=schema, options=options, spark=spark)


def _read_batch(
    src: FileSharePath | str,
    file_format: str,
    schema: StructType | None = None,
    options: dict[str, str | bool | int] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    spark = _ensure_spark(spark)

    if file_format == "table":
        assert isinstance(src, str)
        return spark.read.table(src)

    path_glob_filter = file_format
    file_format = "binaryFile" if file_format == "pdf" else file_format

    if isinstance(src, str):
        src = FileSharePath(src)

    reader = spark.read.format(file_format)
    reader = reader.option("pathGlobFilter", f"*.{path_glob_filter}")

    if schema:
        reader = reader.schema(schema)

    # default options
    reader = reader.option("recursiveFileLookup", "True")
    if file_format == "parquet":
        reader = reader.option("mergeSchema", "true")
    if file_format == "csv":
        reader = reader.option("header", "true")

    # custom / override options
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)

    return reader.load(src.string)


@overload
def read(
    stream: bool, table: str, *, metadata: bool | None = False, spark: SparkSession | None = None
) -> DataFrame: ...


@overload
def read(
    stream: bool,
    *,
    path: FileSharePath | str,
    file_format: str = "delta",
    metadata: bool | None = False,
    spark: SparkSession | None = None,
) -> DataFrame: ...


@overload
def read(
    stream: bool,
    *,
    path: FileSharePath | str,
    file_format: str,
    schema: StructType,
    options: dict[str, str | bool | int] | None = None,
    metadata: bool | None = True,
    spark: SparkSession | None = None,
) -> DataFrame: ...


@overload
def read(
    stream: bool,
    *,
    path: FileSharePath | str,
    file_format: str,
    schema_path: FileSharePath | str,
    options: dict[str, str | bool | int] | None = None,
    metadata: bool | None = True,
    spark: SparkSession | None = None,
) -> DataFrame: ...


def read(
    stream: bool,
    table: str | None = None,
    path: FileSharePath | str | None = None,
    file_format: str | None = None,
    schema_path: FileSharePath | str | None = None,
    schema: StructType | None = None,
    hints: str | list[str] | None = None,
    options: dict[str, str | bool | int] | None = None,
    metadata: bool | None = True,
    spark: SparkSession | None = None,
) -> DataFrame:
    spark = _ensure_spark(spark)

    if table is not None:
        file_format = "table"
        src = table
    else:
        assert path
        assert file_format
        src = path

    if stream:
        df = _read_stream(
            src=src,
            file_format=file_format,
            schema_path=schema_path,
            hints=hints,
            schema=schema,
            options=options,
            spark=spark,
        )
    else:
        df = _read_batch(src=src, file_format=file_format, schema=schema, options=options, spark=spark)

    if metadata:
        if stream and file_format == "delta":
            df = df.selectExpr(
                "*",
                """
                struct(
                    cast(null as string) as file_path,
                    cast(null as string) as file_name,
                    cast(null as string) as file_size,
                    cast(null as string) as file_modification_time
                    ) as __metadata
                """,
            )
        else:
            df = df.selectExpr(
                "*",
                """
                struct(
                    _metadata.file_path as file_path,
                    _metadata.file_name as file_name,
                    _metadata.file_size as file_size,
                    _metadata.file_modification_time as file_modification_time
                    ) as __metadata
                """,
            )

    return df
