"""Direct live execution checks for the Bronze, Silver, and Gold layers."""

from fabricks.context import SPARK
from fabricks.core import get_job


def _run_bronze_king():
    job = get_job(step="bronze", topic="king", item="scd1")
    job.register_external_table()
    return job


def _run_silver_king():
    _run_bronze_king()
    job = get_job(step="silver", topic="king", item="scd1")
    job.run()
    return job


def test_bronze_reads_seeded_first_iteration():
    job = _run_bronze_king()

    rows = SPARK.table(job.qualified_name).select("id", "name", "__operation").orderBy("id").collect()
    assert [(row.id, row.name, row["__operation"]) for row in rows] == [
        (1, "Leopold I", "upsert"),
        (2, "Leopold II", "upsert"),
    ]


def test_silver_materializes_first_iteration_cdc():
    job = _run_silver_king()

    rows = job.table.dataframe.where("__is_current and not __is_deleted").select("id", "name").orderBy("id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "Leopold I"), (2, "Leopold II")]


def test_gold_materializes_silver_dependency():
    _run_silver_king()
    get_job(step="gold", topic="dim", item="time").run()
    job = get_job(step="gold", topic="fact", item="dependency")
    job.run()

    assert job.table.dataframe.count() == 120


def test_silver_cdc_matches_first_iteration_expected_state():
    job = _run_silver_king()
    actual = job.table.dataframe.select("id", "name", "__is_current", "__is_deleted").orderBy("id").collect()
    expected = [
        (1, "Leopold I", True, False),
        (2, "Leopold II", True, False),
    ]

    assert [(row.id, row.name, row["__is_current"], row["__is_deleted"]) for row in actual] == expected
