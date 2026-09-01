from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, cast

from databricks.sdk.runtime import dbutils, spark
import pandas as pd
from pyspark.sql.functions import expr
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.path import FileSharePath, GitPath
from tests.databricks._types import paths

_EXPECTED_SCD2_SCHEMA = StructType(
    [
        StructField("__valid_from", TimestampType(), True),
        StructField("__valid_to", TimestampType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)


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

        df = df.withColumn("__split", expr("split(replace(__file_path, __file_name), '/')"))
        df = df.withColumn("__split_size", expr("size(__split)"))
        df = df.withColumn("__timestamp", expr("left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)"))
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
        p_df = pd.read_json(f, orient="records", lines=True, convert_dates=cast(Any, dates))
        df = spark.createDataFrame(p_df)

        folder = str(Path(f).parent)
        to_folder = folder.replace("\\", "/").replace(from_dir.string, to_dir.string)

        DEFAULT_LOGGER.debug(f"{folder} -> {to_folder}")
        df.coalesce(2).write.format("parquet").mode("overwrite").save(to_folder)

        # monarch and regent load
        # custom load for 2022/04/01/0001 as there is a reload for queen and no reload for king
        for t in ["monarch", "regent"]:
            if ("king" in to_folder or "queen" in to_folder) and "2022/04/01/0001" not in str(f):
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

        from_dir = paths.tests.parent().joinpath("data", job)
        to_dir = paths.landing.joinpath(job)

        convert_json_to_parquet(from_dir, to_dir)


def landing_to_raw(iter: int | list[int]):
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
                    to_path = f.replace("landing", "raw").replace(job, "")
                    if i > 1:  # needed for unity catalog (cannot use same delta table more than once)
                        to_path = to_path.replace("raw", f"raw/{i}")
                        print(to_path)

                    to_path = FileSharePath(to_path)

                    dbutils.fs.cp(path.string, to_path.string)

    convert_parquet_to_delta("regent")
    convert_parquet_to_delta("monarch")
    convert_parquet_to_delta("prince", deletelog=False)


def create_expected_views():
    DEFAULT_LOGGER.info("expected - create views")

    def _create_views(step: str, cdc: str):
        views = paths.tests.parent().joinpath("expected", step, cdc)

        if step == "silver" and cdc == "scd2":
            # Only job1's file is hand-authored NDJSON data (see Task 3's
            # Files note) — job2.sql onward are still real SQL, each unioning
            # its OWN new VALUES rows with `select ... from
            # expected.silver_scd2_job{N-1} where not __is_current` (verified:
            # job3.sql references job2, job2.sql references job1 — a genuine
            # sequential chain, not all pointing at job1). So this branch
            # must create job1's NDJSON root *then fall through* to the SQL
            # loop below for job2 onward, in ascending job-number order — an
            # early `return` here would silently skip creating
            # expected.silver_scd2_job{2..9} entirely, since nothing else in
            # this function ever visits this directory's .sql files.
            for v in sorted(views.walk(file_format="ndjson")):
                DEFAULT_LOGGER.debug(f"create table {v}")
                job_num = str(int(re.search(r"\d+", GitPath(v).get_file_name()).group()))
                rows = [json.loads(line) for line in GitPath(v).pathlibpath.read_text().splitlines()]
                for row in rows:
                    # tzinfo=utc pins these as instants (matching the original SQL's
                    # `cast(... as timestamp)` on a UTC-session string) instead of
                    # leaving them naive, which Spark Connect's client-side row->Arrow
                    # conversion would otherwise localize using the driver machine's
                    # OS timezone (silently shifting every value, and raising OSError
                    # for the far-future '9999-12-31' sentinel on Windows drivers).
                    row["__valid_from"] = datetime.strptime(row["__valid_from"], "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=timezone.utc
                    )
                    row["__valid_to"] = datetime.strptime(row["__valid_to"], "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=timezone.utc
                    )
                df = spark.createDataFrame(rows, schema=_EXPECTED_SCD2_SCHEMA)
                df.write.mode("overwrite").saveAsTable(f"expected.silver_scd2_job{job_num}")

        for v in sorted(views.walk(file_format="sql")):
            DEFAULT_LOGGER.debug(f"create view {v}")
            spark.sql(GitPath(v).get_sql())

    _create_views("silver", "scd2")
    _create_views("silver", "scd1")

    _create_views("gold", "scd2")
    _create_views("gold", "scd1")
    _create_views("gold", "scd0")
