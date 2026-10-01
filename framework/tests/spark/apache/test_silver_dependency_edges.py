"""Silver dependency resolution persisted for real: every edge of the local runtime with its origin, and a second
write that does not duplicate them. `Silver.get_dependencies()` never reads a .sql file, so this runs locally; the
Databricks tier repeats it against a live workspace with its own runtime."""

from fabricks.core.steps import get_step
from fabricks.models import get_job_id

# (job, parent, origin): `parser` is the same-name bronze default, `parent` an explicit `parents` entry
_EXPECTED = {
    ("silver.append_test_test", "bronze.append_test_test", "parser"),
    ("silver.extender_test_source", "bronze.extender_test_source", "parser"),
    ("silver.king_and_queen_scd1", "bronze.king_scd1", "parent"),
    ("silver.king_and_queen_scd1", "bronze.queen_scd1", "parent"),
    ("silver.king_and_queen_scd2", "bronze.king_scd1", "parent"),
    ("silver.king_and_queen_scd2", "bronze.queen_scd1", "parent"),
    ("silver.latest_test_test", "bronze.latest_test_test", "parser"),
}


def test_update_dependencies_persists_every_silver_edge_exactly_once(local_spark):
    step = get_step("silver")
    step.update_configurations()

    step.update_dependencies()
    step.update_dependencies()

    rows = local_spark.sql("select job_id, parent, origin from fabricks.silver_dependencies").collect()
    assert len(rows) == len(_EXPECTED), "a second write must not duplicate edges"
    assert {(row.job_id, row.parent, row.origin) for row in rows} == {
        (get_job_id(job=job), parent, origin) for job, parent, origin in _EXPECTED
    }
