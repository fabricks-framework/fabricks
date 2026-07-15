import os
import re
from typing import Any, List, Union, cast

import pandas as pd
from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import concat_dfs, run_in_parallel
from fabricks.utils.path import FileSharePath, GitPath
from tests.integration.helpers.const import LANDING, RAW, ROOT

_DATES = ["BEL_DeleteDateUtc", "BEL_RestoredDateUtc", "BEL_UpdateDateUtc"]
_TOPIC_SOURCES = {
    "king": ["king", "king__deletelog"],
    "queen": ["queen", "queen__deletelog"],
    "prince": ["prince"],
    "princess": ["princess"],
    **{t: ["king", "queen", "king__deletelog", "queen__deletelog"] for t in ["monarch", "regent", "royal"]},
}
_TOPIC_OPERATIONS = {"royal": "reload"}
# job4's single file: single-source, not aliased into the merged monarch/regent/royal topics
_NO_ALIAS_MARKER = "2022/04/01/0001"


def _convert_parquet_to_delta(topic: str, deletelog: bool = True):
    for i in range(1, 5):
        dfs = []
        root = RAW

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
        df = _add_timestamp(df)
        df = _drop_extra__columns(df)

        if topic in ["royal"]:
            df = _force_operation(df, topic)

        writer = df.write.mode("append").option("mergeSchema", "True").format("delta")

        if any(not re.match(r"^[a-zA-Z0-9_]+$", c) for c in df.columns):
            writer = writer.option("delta.columnMapping.mode", "name")

        writer.save(f"{root}/delta/{topic}")


def _force_operation(df: DataFrame, topic: str) -> DataFrame:
    operation = _TOPIC_OPERATIONS.get(topic)

    if operation:
        df = df.withColumn("__operation", lit(operation))

    return df


def _add_timestamp(df: DataFrame) -> DataFrame:
    df = df.withColumn("__split", expr("split(replace(__file_path, __file_name), '/')"))
    df = df.withColumn("__split_size", expr("size(__split)"))
    df = df.withColumn("__timestamp", expr("left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)"))
    df = df.withColumn("__timestamp", expr("to_timestamp(__timestamp, 'yyyyMMddHHmmss')"))

    return df


def _drop_extra__columns(df: DataFrame) -> DataFrame:
    for c in ["__file_path", "__file_name", "__split", "__split_size"]:
        if c in df.columns:
            df = df.drop(c)

    return df


def _alias_paths(path: str) -> list[str]:
    for source in ("king", "queen"):
        if source in path:
            return [path.replace(source, t) for t in ["monarch", "regent", "royal"]]

    return []


def _convert_json_to_parquet(from_dir: GitPath, to_dir: FileSharePath):
    DEFAULT_LOGGER.debug(f"converting json to parquet - {to_dir}")
    files = from_dir.walk()

    for f in files:
        p_df = pd.read_json(f, orient="records", convert_dates=cast(Any, _DATES))
        df = spark.createDataFrame(p_df)
        folder = os.path.dirname(f)
        to_folder = folder.replace("\\", "/").replace(from_dir.string, to_dir.string)
        df.coalesce(2).write.format("parquet").mode("overwrite").save(to_folder)

        if _NO_ALIAS_MARKER not in str(f):
            for to_folder_ in _alias_paths(to_folder):
                df.coalesce(1).write.format("parquet").mode("append").save(to_folder_)


def _seed_job_dirs():
    # single source of truth: derived from the seed dir, so a new job dir is never silently skipped
    return sorted(ROOT.joinpath("seed").pathlibpath.glob("job*"), key=lambda p: int(p.name[3:]))


def git_to_landing():
    DEFAULT_LOGGER.info("moving data from git to landing")
    seed_dir = ROOT.joinpath("seed")

    for job_dir in _seed_job_dirs():
        job = job_dir.name
        DEFAULT_LOGGER.debug(f"copying json from git to landing ({job})")
        _convert_json_to_parquet(seed_dir.joinpath(job), LANDING.joinpath(job))


def landing_to_raw(iter: Union[int, List[int]]):
    DEFAULT_LOGGER.info("moving data from landing to raw")

    if isinstance(iter, int):
        iter = [iter]

    for i in iter:
        job = f"job{i}"
        DEFAULT_LOGGER.debug(f"copying parquet from landing to raw ({job})")
        landing = LANDING.joinpath(job)

        for f in landing.walk():
            if str(f).endswith("parquet"):
                path = FileSharePath(f)

                for j in range(1, 4):
                    to_path = f.replace(f"landing/{job}/", "raw/")

                    if j > 1:  # needed for unity catalog (cannot use same delta table more than once)
                        to_path = to_path.replace("raw", f"raw/{j}")

                    dbutils.fs.cp(path.string, FileSharePath(to_path).string)

                    if _NO_ALIAS_MARKER not in str(f):
                        for to_path_ in _alias_paths(to_path):
                            dbutils.fs.cp(path.string, FileSharePath(to_path_).string)

    _convert_parquet_to_delta("regent")
    _convert_parquet_to_delta("monarch")
    _convert_parquet_to_delta("prince", deletelog=False)
    _convert_parquet_to_delta("royal", deletelog=False)


def create_input_tables(topics: List[str] | None = None):
    DEFAULT_LOGGER.info("input - creating tables")

    if topics is None:
        topics = ["monarch", "prince", "princess", "king", "queen", "royal", "regent"]

    spark.sql("create schema if not exists input")
    job_dirs = _seed_job_dirs()

    def _write_table(rows: List[Any], topic: str, table: str):
        p_df = pd.concat(rows, ignore_index=True)
        df = spark.createDataFrame(p_df)

        if topic in ["royal"]:
            df = _force_operation(df, topic)
        elif "__operation" not in df.columns:
            if "BEL_IsFullLoad" in df.columns:
                df = df.withColumn(
                    "__operation",
                    expr(
                        "if(BEL_DeleteDateUtc is not null, 'delete', if(BEL_IsFullLoad=='true', 'reload', 'upsert'))"
                    ),
                )
            else:
                df = df.withColumn("__operation", expr("if(__file_path like '%deletelog%', 'delete', 'upsert')"))

        df: DataFrame = _add_timestamp(df)
        cols = [c for c in df.columns if c.startswith("BEL_")]

        if cols:
            df = df.drop(*cols)
            df = _drop_extra__columns(df)

        (
            df.write.mode("overwrite")
            .option("overwriteSchema", "True")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .saveAsTable(table)
        )

    def _create_tables(topic: str):
        accumulated: List[Any] = []

        for job_dir in job_dirs:
            DEFAULT_LOGGER.debug(f"creating table input.{topic}_{job_dir.name}")
            scan_dirs = [job_dir / s for s in _TOPIC_SOURCES.get(topic, [topic])]
            batch: List[Any] = []

            for scan_dir in scan_dirs:
                for json_file in sorted(scan_dir.rglob("*.json")):
                    p_df = pd.read_json(str(json_file), orient="records", convert_dates=cast(Any, _DATES))
                    p_df["__file_path"] = str(json_file).replace("\\", "/")
                    p_df["__file_name"] = json_file.name
                    batch.append(p_df)

            accumulated.extend(batch)

            if not accumulated:
                continue

            _write_table(accumulated, topic, f"input.{topic}_{job_dir.name}")

            if batch:
                _write_table(batch, topic, f"input.{topic}_{job_dir.name}_batch")

    run_in_parallel(_create_tables, topics)


def create_expected_views():
    DEFAULT_LOGGER.info("expected - creating views")

    def _create_views(step: str, cdc: str):
        DEFAULT_LOGGER.debug(f"creating expected views for {step} - {cdc}")
        views = ROOT.joinpath("expected", step, cdc)

        for v in sorted(views.walk()):
            spark.sql(GitPath(v).get_sql())

    # latest and append both derive from the scd2 views, dropping the history columns;
    # latest additionally keeps only the last version.
    except_cols = "except(__valid_from, __valid_to, __is_current, __is_deleted)"

    def _create_derived_views(step: str, suffix: str):
        DEFAULT_LOGGER.debug(f"creating expected {suffix} views for {step}")
        views = ROOT.joinpath("expected", step, "scd2")

        for v in sorted(views.walk()):
            job_n = int(str(v).split("job")[-1].split(".")[0])
            source = f"expected.{step}_scd2_job{job_n}"
            target = f"expected.{step}_{suffix}_job{job_n}"
            where = f" where __valid_from = (select max(__valid_from) from {source})" if suffix == "latest" else ""
            spark.sql(f"create or replace view {target} as select * {except_cols} from {source}{where}")

    # silver
    _create_views("silver", "scd2")
    _create_views("silver", "scd1")
    # gold
    _create_views("gold", "scd2")
    _create_views("gold", "scd1")
    _create_views("gold", "scd0")
    # latest
    _create_derived_views("silver", "latest")
    _create_derived_views("gold", "latest")
    # append
    _create_derived_views("silver", "append")
    _create_derived_views("gold", "append")


def create_random_tables():
    if CATALOG:
        spark.sql(f"use catalog {CATALOG}")

    spark.sql("create schema if not exists bronze")
    uri = f"{RAW}/delta/no_column"
    spark.sql(f"create table if not exists bronze.princess_no_column using delta location '{uri}'")
