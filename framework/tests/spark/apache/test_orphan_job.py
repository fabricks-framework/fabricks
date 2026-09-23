"""Reproduces https://github.com/fabricks-framework/fabricks/issues/198:
a job removed from the runtime without calling drop() first leaves its
Delta table/view and schema/checkpoint folders orphaned -- get_job()
can no longer build a real job object for it (no config to resolve), so
the regular drop() path (fabricks/core/jobs/base/generator.py) is
unreachable. OrphanJob (fabricks/core/jobs/orphan.py) is built purely
from step/topic/item -- no config lookup -- so it can drop these even
when no runtime config for the job exists at all, which this test never
creates one for.
"""

from fabricks.cdc import NoCDC
from fabricks.context import PATHS_STORAGE
from fabricks.core.jobs import get_orphan_job


def test_orphan_job_drops_an_orphaned_table_and_its_folders(local_spark):
    cdc = NoCDC("bronze", "orphan_test", "table_job", spark=local_spark)
    cdc.table.create(local_spark.createDataFrame([(1, "a")], ["id", "name"]))
    assert cdc.table.exists()

    storage = PATHS_STORAGE.get("bronze")
    assert storage
    schema_path = storage.joinpath("schema", "orphan_test", "table_job")
    checkpoints_path = storage.joinpath("checkpoints", "orphan_test", "table_job")
    schema_path.pathlibpath.mkdir(parents=True, exist_ok=True)
    (schema_path.pathlibpath / "dummy.txt").write_text("x")
    checkpoints_path.pathlibpath.mkdir(parents=True, exist_ok=True)
    (checkpoints_path.pathlibpath / "dummy.txt").write_text("x")

    get_orphan_job(step="bronze", topic="orphan_test", item="table_job").drop()

    assert not cdc.table.exists()
    assert not schema_path.exists()
    assert not checkpoints_path.exists()


def test_orphan_job_drops_an_orphaned_view(local_spark):
    local_spark.sql("create or replace view silver.orphan_test_view_job as select 1 as id")
    assert local_spark.catalog.tableExists("silver.orphan_test_view_job")

    get_orphan_job(step="silver", topic="orphan_test", item="view_job").drop()

    assert not local_spark.catalog.tableExists("silver.orphan_test_view_job")
