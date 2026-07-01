import os
import re
from typing import Any, List, Union, cast

import pandas as pd
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql.functions import expr, lit

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.path import FileSharePath, GitPath
from tests.integration._types import paths


def create_random_tables():
    if CATALOG:
        spark.sql(f"use catalog {CATALOG}")

    spark.sql("create schema if not exists bronze")
    uri = f"{paths.raw}/delta/no_column"
    spark.sql(f"create table if not exists bronze.princess_no_column using delta location '{uri}'")


def convert_parquet_to_delta(topic: str, deletelog: bool = True):
    for i in range(1, 4):
        dfs = []
        root = paths.raw

        if i > 1:
            root = root.joinpath(str(i))

        _paths = [topic]

        if deletelog:
            _paths.append(f"{topic}__deletelog")

        for p in _paths:
            df = (
                spark.read.option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "True")
                .option("mergeSchema", "True")
                .parquet(f"{root}/{p}")
            )
            df = df.selectExpr(
                "*",
                "cast(10.52 as decimal (10,1)) as decimalField",
                "_metadata.file_path as __file_path",
                "_metadata.file_name as __file_name",
            )
            dfs.append(df)

        df = concat_dfs(dfs)
        assert df is not None

        if topic == "duke":
            df = df.withColumn("__operation", lit("reload"))

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
        writer = df.write.mode("append").option("mergeSchema", "True").format("delta")

        if any(not re.match(r"^[a-zA-Z0-9_]+$", c) for c in df.columns):
            writer = writer.option("delta.columnMapping.mode", "name")

        writer.save(f"{root}/delta/{topic}")


def convert_json_to_parquet(from_dir: GitPath, to_dir: FileSharePath):
    DEFAULT_LOGGER.debug(f"convert json to parquet - {to_dir}")
    dates = ["BEL_DeleteDateUtc", "BEL_RestoredDateUtc", "BEL_UpdateDateUtc"]
    files = from_dir.walk()

    for f in files:
        p_df = pd.read_json(f, orient="records", convert_dates=cast(Any, dates))
        df = spark.createDataFrame(p_df)
        folder = os.path.dirname(f)
        to_folder = folder.replace("\\", "/").replace(from_dir.string, to_dir.string)
        DEFAULT_LOGGER.debug(f"{folder} -> {to_folder}")
        df.coalesce(2).write.format("parquet").mode("overwrite").save(to_folder)

        # monarch and regent load
        # custom load for 2022/04/01/0001 as there is a reload for queen and no reload for king
        for t in ["monarch", "regent", "duke"]:
            if "king" in to_folder or "queen" in to_folder:
                if "2022/04/01/0001" not in str(f):
                    to_folder_ = to_folder

                    if "king" in to_folder:
                        to_folder_ = to_folder_.replace("king", t)
                    elif "queen" in to_folder:
                        to_folder_ = to_folder_.replace("queen", t)

                    DEFAULT_LOGGER.debug(f"{folder} -> {to_folder_}")
                    df.coalesce(1).write.format("parquet").mode("append").save(to_folder_)


def git_to_landing():
    DEFAULT_LOGGER.info("git to landing")

    for i in range(1, 12):
        job = f"job{i}"
        DEFAULT_LOGGER.debug(f"copy json from git to landing ({job})")
        from_dir = paths.tests.joinpath("data", job)
        to_dir = paths.landing.joinpath(job)
        convert_json_to_parquet(from_dir, to_dir)


def landing_to_raw(iter: Union[int, List[int]]):
    DEFAULT_LOGGER.info("landing to raw")

    if isinstance(iter, int):
        iter = [iter]

    for i in iter:
        job = f"job{i}"
        DEFAULT_LOGGER.debug(f"copy parquet from landing to raw ({job})")
        landing = paths.landing.joinpath(job)

        for f in landing.walk():
            if str(f).endswith("parquet"):
                path = FileSharePath(f)

                for i in range(1, 4):
                    to_path = f.replace(f"landing/{job}/", "raw/")

                    if i > 1:  # needed for unity catalog (cannot use same delta table more than once)
                        to_path = to_path.replace("raw", f"raw/{i}")
                        print(to_path)

                    to_path = FileSharePath(to_path)
                    dbutils.fs.cp(path.string, to_path.string)

    convert_parquet_to_delta("regent")
    convert_parquet_to_delta("monarch")
    convert_parquet_to_delta("prince", deletelog=False)
    convert_parquet_to_delta("duke", deletelog=False)


def create_input_views(topics: List[str] | None = None):
    DEFAULT_LOGGER.info("input - create views")

    if topics is None:
        topics = ["monarch", "prince", "princess"]

    spark.sql("create schema if not exists input")
    dates = ["BEL_DeleteDateUtc", "BEL_RestoredDateUtc", "BEL_UpdateDateUtc"]
    data_dir = paths.tests.joinpath("data")
    job_dirs = sorted(data_dir.pathlibpath.glob("job*"), key=lambda p: int(p.name[3:]))

    for topic in topics:
        accumulated: List[Any] = []

        for job_dir in job_dirs:
            topic_dir = job_dir / topic

            if topic_dir.exists():
                for json_file in sorted(topic_dir.rglob("*.json")):
                    p_df = pd.read_json(str(json_file), orient="records", convert_dates=cast(Any, dates))
                    p_df["__job"] = job_dir.name
                    accumulated.append(p_df)

            if not accumulated:
                continue

            p_df = pd.concat(accumulated, ignore_index=True)
            df = spark.createDataFrame(p_df)
            (
                df.write.mode("overwrite")
                .option("overwriteSchema", "True")
                .option("delta.columnMapping.mode", "name")
                .option("delta.minReaderVersion", "2")
                .option("delta.minWriterVersion", "5")
                .saveAsTable(f"input.{topic}_{job_dir.name}")
            )
            DEFAULT_LOGGER.debug(f"created table input.{topic}_{job_dir.name}")


def create_expected_views():
    DEFAULT_LOGGER.info("expected - create views")

    def _create_views(step: str, cdc: str):
        views = paths.tests.joinpath("expected", step, cdc)

        for v in sorted(views.walk()):
            DEFAULT_LOGGER.debug(f"create view {v}")
            spark.sql(GitPath(v).get_sql())

    _create_views("silver", "scd2")
    _create_views("silver", "scd1")
    _create_views("gold", "scd2")
    _create_views("gold", "scd1")
    _create_views("gold", "scd0")
