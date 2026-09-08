"""Proves get_job() resolves real YAML config and drives real local
Spark+Delta state -- not just config parsing (already covered, Spark faked,
by tests/unit/config/). test_cdc.py's own SCD1/SCD2/SCD0/NoCDC tests
deliberately bypass get_job() entirely (see docs/adr/0001-...); this file is
the one place in tests/spark/apache/ that goes through the real job-config-
resolution + orchestration-adjacent path instead.
"""

from fabricks.core import get_job


def test_get_job_bronze_king_only_reads_real_registered_delta_table(
    local_spark, king_and_queen_registered_sources
):
    job = get_job(step="bronze", topic="king", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from iter1/bronze_king.jsonl"
    assert "id" in df.columns


def test_get_job_bronze_queen_only_reads_real_registered_delta_table(
    local_spark, king_and_queen_registered_sources
):
    job = get_job(step="bronze", topic="queen", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from iter1/bronze_queen.jsonl"
    assert "id" in df.columns
