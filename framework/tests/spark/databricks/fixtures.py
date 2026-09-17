"""Raw/Delta fixture seeding for the Databricks test tier's runtests.py.
Split out of that notebook (and, for registered_delta_rows, out of
tests/spark/test_data.py, which re-exports it) to keep the notebook
orchestration-only. This notebook runs from bundle-synced workspace files,
not a Databricks Repo, so `tests` itself isn't importable there -- Databricks
adds a notebook's own containing folder to sys.path, not distant ancestors.
"""

from typing import Literal

_REGISTERED_DELTA_ROWS = {
    "king": (
        {"id": 1, "name": "Leopold I", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
        {"id": 2, "name": "Leopold II", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},
    ),
    "queen": ({"id": 101, "name": "Louise", "__operation": "upsert", "__timestamp": "2022-01-01T00:01:00"},),
}


def registered_delta_rows(entity: Literal["king", "queen"]) -> list[dict]:
    return [dict(row) for row in _REGISTERED_DELTA_ROWS[entity]]


def seed_raw_fixtures() -> None:
    """Copy the checked-in iter1 king fixtures into the real raw/king path
    bronze.feature_parser reads from. ponytail: plain file copy, no
    landing/parquet conversion stage like the old databricks-old pipeline --
    the job's "dummy" parser reads it directly, so the git-checked-in
    fixtures work as-is.
    """
    from databricks.sdk.runtime import dbutils

    from fabricks.context import PATH_RUNTIME
    from fabricks.core import get_job

    job = get_job(step="bronze", topic="feature", item="parser")
    king_fixtures = PATH_RUNTIME.parent().parent().joinpath("fixtures", "iter1", "king")
    job.data_path.rm()
    for f in king_fixtures.walk(convert=True, file_format="jsonl"):
        rel = f.string.removeprefix(f"{king_fixtures.string}/")
        dbutils.fs.cp(f"file:{f.string}", job.data_path.joinpath(rel).string)


def seed_raw_delta_fixtures() -> None:
    """Write a small Delta table at king/queen's raw uri -- Bronze.
    register_external_table() (bronze.py) selects from that uri directly, so
    it needs a real table there, same shape as tests/spark/apache/conftest.py's
    king_and_queen_registered_sources.
    """
    from pyspark.sql.functions import col, lit

    from fabricks.context import SPARK
    from fabricks.core import get_job

    for topic in ("king", "queen"):
        job = get_job(step="bronze", topic=topic, item="scd1")
        df = SPARK.createDataFrame(registered_delta_rows(topic))
        df = df.withColumn("__source", lit(topic)).withColumn("__timestamp", col("__timestamp").cast("timestamp"))
        df.write.format("delta").mode("overwrite").save(job.data_path.string)
