from fabricks.core.steps import get_step
from fabricks.models import get_dependency_id, get_job_id


def test_update_configurations_writes_gold_jobs(local_spark):
    step = get_step("gold")
    expected = step.get_jobs().count()

    step.update_configurations()

    assert local_spark.sql("select * from fabricks.gold_jobs").count() == expected


def test_update_dependencies_writes_silver_dependencies(local_spark):
    # Silver.get_dependencies() never reads a .sql file, so the DDL-only gold jobs' missing .sql don't matter.
    step = get_step("silver")
    step.update_configurations()

    expected_df, errors = step._get_dependencies_internal()
    assert errors == []

    step.update_dependencies()

    assert local_spark.sql("select * from fabricks.silver_dependencies").count() == expected_df.count()


def test_create_db_objects_materializes_runnable_semantic_jobs(local_spark):
    step = get_step("semantic")

    step.create_db_objects()
    step.create_db_objects()

    assert local_spark.catalog.tableExists("semantic.fact_table")


# https://github.com/fabricks-framework/fabricks/issues/183: memory views with dependencies failed with
# TABLE_OR_VIEW_NOT_FOUND on first deployment. parallel=False makes the first pass fail deterministically.
def test_create_db_objects_resolves_a_memory_view_dependency_chain(local_spark, monkeypatch):
    step = get_step("gold")
    get_depchain_jobs = step.get_jobs
    monkeypatch.setattr(step, "get_jobs", lambda topic=None: get_depchain_jobs(topic="depchain"))

    step.create_db_objects(parallel=False)

    assert local_spark.catalog.tableExists("gold.depchain_view_a")
    assert local_spark.catalog.tableExists("gold.depchain_view_b")
    assert local_spark.catalog.tableExists("gold.depchain_view_c")
    assert local_spark.catalog.tableExists("gold.depchain_view_d")
    assert local_spark.sql("select * from gold.depchain_view_a").collect() == [(1,)]
    assert local_spark.sql("select * from gold.depchain_view_d").collect() == [(1, 1)]


def test_update_dependencies_persists_stable_semantic_ids(local_spark):
    step = get_step("semantic")
    job_id = get_job_id(step="semantic", topic="fact", item="dependency")
    parent = "gold.fact_parent"

    step.update_dependencies()
    rows = local_spark.sql(
        f"select dependency_id, job_id, parent, origin from fabricks.semantic_dependencies where job_id = '{job_id}'"
    ).collect()

    assert [(row.dependency_id, row.job_id, row.parent, row.origin) for row in rows] == [
        (get_dependency_id(parent, job_id), job_id, parent, "parent")
    ]


def test_update_configurations_repairs_deleted_and_stale_rows(local_spark):
    step = get_step("semantic")
    step.update_configurations()
    expected = step.get_jobs().count()

    local_spark.sql("delete from fabricks.semantic_jobs where item = 'table'")
    local_spark.sql("update fabricks.semantic_jobs set item = 'stale' where item = 'zstd'")
    step.update_configurations()

    rows = local_spark.sql("select item from fabricks.semantic_jobs").collect()
    assert len(rows) == expected
    assert {row.item for row in rows} >= {"table", "zstd"}
