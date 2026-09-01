from typing import get_args

from pyspark.sql import DataFrame

from fabricks.utils.path import FileSharePath
from fabricks.utils.read._types import AllowedIOModes


def write_delta(
    df: DataFrame,
    path: FileSharePath,
    mode: AllowedIOModes,
    options: dict[str, str] | None = None,
    partition_by: list[str] | None | str = None,
) -> None:
    assert mode in list(get_args(AllowedIOModes))

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    writer = df.write.format("delta").mode(mode).option("mergeSchema", "True").option("overwriteSchema", "True")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if options:
        for key, value in options.items():
            writer = writer.option(key, value)

    writer.save(path.string)


def append_delta(
    df: DataFrame,
    path: FileSharePath,
    options: dict[str, str] | None = None,
    partition_by: list[str] | None | str = None,
) -> None:
    write_delta(df, path, "append", options=options, partition_by=partition_by)


def overwrite_delta(
    df: DataFrame,
    path: FileSharePath,
    options: dict[str, str] | None = None,
    partition_by: list[str] | None | str = None,
) -> None:
    write_delta(df, path, "overwrite", options=options, partition_by=partition_by)
