import json
import os
import re
from typing import Any, List, Union

import pandas as pd
from databricks.sdk.runtime import dbutils, spark

from fabricks.context import CATALOG
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import concat_dfs, run_in_parallel
from fabricks.utils.path import FileSharePath, GitPath
from tests.integration.helpers.const import LANDING, RAW, RAW_DATA, ROOT

RAW_ITERATIONS = range(1, 5)


def _data_job_dirs():
    return sorted(RAW_DATA.pathlibpath.glob("job*"), key=lambda p: int(p.name[3:]))


def _convert_parquet_to_delta(topic: str, deletelog: bool = False):
    paths = [topic] + ([f"{topic}__deletelog"] if deletelog else [])

    for i in RAW_ITERATIONS:
        root = RAW if i == 1 else RAW.joinpath(str(i))

        def _exists(p: str) -> bool:  # ponytail: dbutils.fs.ls throws on missing path
            try:
                return len(list(dbutils.fs.ls(f"{root}/{p}"))) > 0
            except Exception:
                return False

        df = concat_dfs(
            [
                spark.read.option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "True")
                .option("mergeSchema", "True")
                .parquet(f"{root}/{p}")
                for p in paths
                if _exists(p)
            ]
        )
        assert df is not None
        writer = df.write.mode("append").option("mergeSchema", "True").format("delta")

        if any(not re.match(r"^[a-zA-Z0-9_]+$", c) for c in df.columns):
            writer = writer.option("delta.columnMapping.mode", "name")

        writer.save(f"{root}/delta/{topic}")


def _convert_json_to_parquet(from_dir: GitPath, to_dir: FileSharePath):
    for f in from_dir.walk():
        df = spark.createDataFrame(pd.read_json(f, orient="records", convert_dates=False))
        to_folder = os.path.dirname(f).replace("\\", "/").replace(from_dir.string, to_dir.string)
        df.coalesce(2).write.format("parquet").mode("overwrite").save(to_folder)


def git_to_landing():
    DEFAULT_LOGGER.info("moving raw fixtures to landing")

    for job_dir in _data_job_dirs():
        _convert_json_to_parquet(RAW_DATA.joinpath(job_dir.name), LANDING.joinpath(job_dir.name))


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

                for j in RAW_ITERATIONS:
                    to_path = f.replace(f"landing/{job}/", "raw/")

                    if j > 1:  # needed for unity catalog (cannot use same delta table more than once)
                        to_path = to_path.replace("raw", f"raw/{j}")

                    dbutils.fs.cp(path.string, FileSharePath(to_path).string)

    for topic in ["monarch", "regent", "prince", "royal"]:
        try:
            dbutils.fs.ls(f"{RAW}/{topic}")
        except Exception as e:
            raise FileNotFoundError(f"landing_to_raw: {topic} missing in {RAW}") from e

    # monarch alone keeps a separate deletelog folder to merge in (regent/royal merged it
    # already; prince's deletelog is a standalone fixture, excluded from its delta)
    _convert_parquet_to_delta("monarch", deletelog=True)
    _convert_parquet_to_delta("regent")
    _convert_parquet_to_delta("prince")
    _convert_parquet_to_delta("royal")


def create_input_tables(topics: List[str] | None = None):
    DEFAULT_LOGGER.info("input - creating tables")

    if topics is None:
        topics = ["monarch", "prince", "princess", "king", "queen", "royal", "regent"]

    spark.sql("create schema if not exists input")
    job_dirs = _data_job_dirs()

    def _write_table(df, table: str):
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
            batch: List[Any] = []
            scan_dirs = [job_dir / topic]
            deletelog = job_dir / f"{topic}__deletelog"

            if topic != "prince" and deletelog.exists():
                scan_dirs.append(deletelog)

            for scan_dir in scan_dirs:
                for json_file in sorted(scan_dir.rglob("*.json")):
                    with open(json_file) as f:
                        batch.extend(json.load(f))

            accumulated.extend(batch)

            if not accumulated:
                continue

            _write_table(spark.createDataFrame(accumulated), f"input.{topic}_{job_dir.name}")

            if batch:
                _write_table(spark.createDataFrame(batch), f"input.{topic}_{job_dir.name}_batch")

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
