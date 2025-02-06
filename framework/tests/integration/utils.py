import os
from typing import Any, List, Union, cast

import pandas as pd
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.functions import expr

from fabricks.context.log import Logger
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.path import Path
from tests.integration._types import paths


def create_empty_delta():
    uri = f"{paths.raw}/delta/empty"
    spark.sql(f"create table if not exists bronze_princess_no_column using delta location '{uri}'")


def convert_parquet_to_delta(topic: str):
    dfs = []

    for p in [f"{topic}__deletelog", topic]:
        df = (
            spark.read.option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "True")
            .option("mergeSchema", "True")
            .parquet(f"{paths.raw}/{p}")
        )
        df = df.selectExpr(
            "*",
            "cast(10.52 as decimal (10,1)) as decimalField",
            "_metadata.file_path as __file_path",
            "_metadata.file_name as __file_name",
        )
        dfs.append(df)

    df = concat_dfs(dfs)
    df = df.withColumn(
        "__split",
        expr("split(replace(__file_path, __file_name), '/')"),
    )
    df = df.withColumn("__split_size", expr("size(__split)"))
    df = df.withColumn(
        "__timestamp",
        expr("left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)"),
    )
    df = df.withColumn("__timestamp", expr("to_timestamp(__timestamp, 'yyyyMMddHHmmss')"))
    df = df.drop("__split", "__split_size", "__file_path", "__file_name")
    df.write.mode("append").option("mergeSchema", "True").format("delta").save(f"{paths.raw}/delta/{topic}")


def convert_json_to_parquet(from_dir: Path, to_dir: Path):
    Logger.debug(f"convert json to parquet - {to_dir}")

    dates = ["BEL_DeleteDateUtc", "BEL_RestoredDateUtc", "BEL_UpdateDateUtc"]
    files = from_dir.walk()
    for f in files:
        p_df = pd.read_json(f, orient="records", convert_dates=cast(Any, dates))
        df = spark.createDataFrame(p_df)

        folder = os.path.dirname(f)
        to_folder = folder.replace("\\", "/").replace(from_dir.string, to_dir.string)

        Logger.debug(f"{folder} -> {to_folder}")
        df.coalesce(2).write.format("parquet").mode("overwrite").save(to_folder)

        # monarch and regent load
        # custom load for 2022/04/01/0001 as there is a reload for queen and no reload for king
        for t in ["monarch", "regent"]:
            if "king" in to_folder or "queen" in to_folder:
                if "2022/04/01/0001" not in str(f):
                    to_folder_ = to_folder
                    if "king" in to_folder:
                        to_folder_ = to_folder_.replace("king", t)
                    elif "queen" in to_folder:
                        to_folder_ = to_folder_.replace("queen", t)

                    Logger.debug(f"{folder} -> {to_folder_}")
                    df.coalesce(1).write.format("parquet").mode("append").save(to_folder_)


def git_to_landing():
    Logger.info("git to landing")
    for i in range(1, 12):
        job = f"job{i}"
        Logger.debug(f"copy json from git to landing ({job})")
        from_dir = paths.tests.join("data", job)
        to_dir = paths.landing.join(job)
        convert_json_to_parquet(from_dir, to_dir)


def landing_to_raw(iter: Union[int, List[int]]):
    Logger.info("landing to raw")

    if isinstance(iter, int):
        iter = [iter]

    for i in iter:
        job = f"job{i}"
        Logger.debug(f"copy parquet from landing to raw ({job})")

        landing = paths.landing.join(job)
        for f in landing.walk():
            if str(f).endswith("parquet"):
                path = Path(f)
                to_path = Path(f.replace("landing", "raw").replace(job, ""))
                dbutils.fs.cp(path.string, to_path.string)

    convert_parquet_to_delta("regent")
    convert_parquet_to_delta("monarch")
    create_empty_delta()


def create_expected_views():
    Logger.info("expected - create views")

    def _create_views(step: str, cdc: str):
        views = paths.tests.join("expected", step, cdc)
        for v in sorted(views.walk()):
            Logger.debug(f"create view {v}")
            spark.sql(Path(v).get_sql())

    _create_views("silver", "scd2")
    _create_views("silver", "scd1")

    _create_views("gold", "scd2")
    _create_views("gold", "scd1")
