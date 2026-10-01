"""Reads of already-scheduled Bronze/Silver/Gold state.

king/dim_time/fact_dependency are all tagged and run once by the schedule
(conftest.py's _schedule_run) -- these tests just get a job handle and read
what's already there, not re-run anything.
"""

from fabricks.context import SPARK
from fabricks.core import get_job


def test_bronze_reads_seeded_first_iteration():
    job = get_job(step="bronze", topic="king", item="scd1")

    rows = SPARK.table(job.qualified_name).select("id", "name", "__operation").orderBy("id").collect()
    assert [(row.id, row.name, row["__operation"]) for row in rows] == [
        (1, "Leopold I", "upsert"),
        (2, "Leopold II", "upsert"),
    ]


def test_gold_materializes_silver_dependency():
    # gold.fact_dependency cross joins the "10" hour of gold.dim_time (one row per minute) with the current rows
    # of silver.king_scd1
    job = get_job(step="gold", topic="fact", item="dependency")
    minutes = get_job(step="gold", topic="dim", item="time").table.dataframe.where("hour = '10'").count()
    kings = (
        get_job(step="silver", topic="king", item="scd1")
        .table.dataframe.where("__is_current and not __is_deleted")
        .count()
    )

    assert minutes * kings > 0, "the inputs must not be empty, or the product is vacuously equal"
    assert job.table.dataframe.count() == minutes * kings


def test_silver_cdc_matches_first_iteration_expected_state():
    job = get_job(step="silver", topic="king", item="scd1")
    actual = job.table.dataframe.select("id", "name", "__is_current", "__is_deleted").orderBy("id").collect()
    expected = [(1, "Leopold I", True, False), (2, "Leopold II", True, False)]

    assert [(row.id, row.name, row["__is_current"], row["__is_deleted"]) for row in actual] == expected
