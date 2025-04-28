from typing import List, Optional, Union, get_args

from pyspark.sql import DataFrame

from fabricks.utils.path import Path
from fabricks.utils.read._types import IOModes


def write_delta(
    df: DataFrame,
    path: Path,
    mode: IOModes,
    options: Optional[dict[str, str]] = None,
    partition_by: Union[Optional[List[str]], str] = None,
):
    assert mode in list(get_args(IOModes))

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
    path: Path,
    options: Optional[dict[str, str]] = None,
    partition_by: Union[Optional[List[str]], str] = None,
):
    write_delta(df, path, "append", options=options)


def overwrite_delta(
    df: DataFrame,
    path: Path,
    options: Optional[dict[str, str]] = None,
    partition_by: Union[Optional[List[str]], str] = None,
):
    write_delta(df, path, "overwrite", options=options)
