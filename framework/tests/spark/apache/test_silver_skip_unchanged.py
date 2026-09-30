"""End-to-end proof (real Spark/Delta) that ordinary Silver runs did not regress and that a genuinely
empty batch surfaces as a "stale" RunStatus through the real dags.run.run() wrapper.

run() reads dbutils lazily from databricks.sdk.runtime; outside Databricks that import tries to
authenticate, so the fixture below swaps in a fake for the duration of each test (auto-restored).
It also swaps out TABLE_LOG_HANDLER, which run() flushes in its `finally`: flushing the real handler
would resolve a real Azure table, which the local storage root of this tier does not have.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from fabricks.core import get_job
from fabricks.core.dags.run import run


@pytest.fixture(autouse=True)
def _fake_databricks_runtime(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", MagicMock(name="fake_databricks_sdk_runtime"))
    with patch("fabricks.core.dags.run.TABLE_LOG_HANDLER"):
        yield


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
