"""Proves get_job() resolves real YAML config and drives real local
Spark+Delta state -- not just config parsing (already covered, Spark faked,
by tests/unit/config/). test_cdc.py's own SCD1/SCD2/SCD0/NoCDC tests
deliberately bypass get_job() entirely (see docs/adr/0001-...); this file is
the one place in tests/spark/apache/ that goes through the real job-config-
resolution + orchestration-adjacent path instead.
"""

import os
from pathlib import Path

from pyspark.sql.functions import col
import pytest

from fabricks.core import get_job
from fabricks.utils.path import resolve_fileshare_path


@pytest.fixture(scope="session")
def king_and_queen_registered_sources(local_spark):
    fixtures = Path(__file__).parent / "fixtures" / "iter1"
    storage = Path(os.environ["FABRICKS_TEST_DISPOSABLE_STORAGE"])
    for entity in ("king", "queen"):
        path = resolve_fileshare_path(str(storage / "bronze_external" / entity))
        df = local_spark.read.json(str(fixtures / f"bronze_{entity}.jsonl"))
        df = df.withColumn("__timestamp", col("__timestamp").cast("timestamp"))
        df.write.format("delta").mode("overwrite").save(path.string)
        get_job(step="bronze", topic=entity, item="scd1").register_external_table()

    return


def test_get_job_bronze_king_only_reads_real_registered_delta_table(local_spark, king_and_queen_registered_sources):
    job = get_job(step="bronze", topic="king", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from iter1/bronze_king.jsonl"
    assert "id" in df.columns


def test_get_job_bronze_queen_only_reads_real_registered_delta_table(local_spark, king_and_queen_registered_sources):
    job = get_job(step="bronze", topic="queen", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from iter1/bronze_queen.jsonl"
    assert "id" in df.columns
