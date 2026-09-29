"""End-to-end proof (real Spark/Delta) that Tasks 1-4's changes to
bronze.py/silver.py/gold.py/job.py/dags/run.py didn't regress ordinary
Silver runs, and that a genuinely empty batch now surfaces as a "stale"
RunStatus through the real dags.run.run() wrapper, not job.run() directly
(job.run() itself has no return value -- it just raises UnchangedWarning,
per Task 3).

Importing fabricks.core.dags.run pulls in fabricks.core.dags.log, whose
module body calls FABRICKS_STORAGE.get_storage_account() -- only
implemented on the real Azure FileSharePath, not the LocalFileSharePath
this tier's FABRICKS_ENVIRONMENT=docker uses (see tests/unit/config/
conftest.py's own docstring for the same issue in that tier). Faking that
one module out, the same way that tier does, is the smallest fix -- no
real Azure Table is needed for this test, only LOGGER/TABLE_LOG_HANDLER.
"""

import sys
from unittest.mock import MagicMock

if "fabricks.core.dags.log" not in sys.modules:
    sys.modules["fabricks.core.dags.log"] = MagicMock(
        name="fake_dags_log",
        LOGGER=MagicMock(name="fake_dags_logger"),
        TABLE_LOG_HANDLER=MagicMock(name="fake_table_log_handler"),
    )

from fabricks.core import get_job  # must follow the sys.modules fake above
from fabricks.core.dags.run import run


def test_dags_run_returns_stale_for_a_genuinely_empty_silver_batch(local_spark, monkeypatch):
    # append_test's own configured data source isn't wired up in this test
    # runtime -- get_data() is the one seam job.run()/for_each_run() use to
    # source a batch, so controlling it here still exercises the real
    # for_each_batch -> real batch_has_data -> real UnchangedWarning ->
    # real job.run()/dags.run.run() chain end to end, same as
    # test_append_mode_accumulates_across_batches feeding for_each_batch
    # directly, just one layer up so dags.run.run()'s own return value can
    # be proven too.
    job = get_job(step="silver", topic="append_test", item="test")
    first_batch = local_spark.createDataFrame([(1, "a")], ["id", "name"])

    # This job/table is shared with test_job_options.py's
    # test_append_mode_accumulates_across_batches (same session-scoped
    # local_spark, same physical Delta table) -- drop before AND after so
    # neither test's row counts depend on which one runs first in the same
    # session.
    if job.table.exists():
        job.table.drop()

    try:
        # is_stream defaults True (see Silver.is_stream) -- table creation
        # would then need a real "fabricks.dummy" streaming placeholder
        # table that isn't part of this test runtime. Not what this test
        # is about: it's proving the stale/ok RunStatus path, independent
        # of streaming.
        monkeypatch.setattr(job, "is_stream", False)
        monkeypatch.setattr(job, "get_data", lambda **_kwargs: first_batch)
        job.create()

        status = run(job=job, schedule_id="unit-test", schedule="unit-test")
        assert status == "ok"
        assert job.table.dataframe.count() == 1

        monkeypatch.setattr(
            job, "get_data", lambda **_kwargs: local_spark.createDataFrame([], schema=first_batch.schema)
        )
        status = run(job=job, schedule_id="unit-test", schedule="unit-test")
        assert status == "stale"
        assert job.table.dataframe.count() == 1
    finally:
        job.table.drop()
