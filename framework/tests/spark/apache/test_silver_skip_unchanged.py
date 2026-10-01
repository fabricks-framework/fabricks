"""End-to-end proof (real Spark/Delta) that ordinary Silver runs did not regress and that a genuinely
empty batch surfaces as a "stale" RunStatus through the real dags.run.run() wrapper.

run() reads dbutils lazily from databricks.sdk.runtime and flushes the real dags log handler in its
`finally`; both need a fake outside Databricks (no workspace to authenticate against, no Azure storage
account). The `semblance` fixture provides them and restores everything after each test.
"""

from fabricks.core.dags.run import run


def test_dags_run_returns_stale_for_a_genuinely_empty_silver_batch(local_spark, monkeypatch, semblance, fresh_job):
    # append_test's own configured data source isn't wired up in this test
    # runtime -- get_data() is the one seam job.run()/for_each_run() use to
    # source a batch, so controlling it here still exercises the real
    # for_each_batch -> real batch_has_data -> real UnchangedWarning ->
    # real job.run()/dags.run.run() chain end to end, same as
    # test_append_mode_accumulates_across_batches feeding for_each_batch
    # directly, just one layer up so dags.run.run()'s own return value can
    # be proven too.
    job = fresh_job("silver", "append_test", "test")
    first_batch = local_spark.createDataFrame([(1, "a")], ["id", "name"])

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

    monkeypatch.setattr(job, "get_data", lambda **_kwargs: local_spark.createDataFrame([], schema=first_batch.schema))
    status = run(job=job, schedule_id="unit-test", schedule="unit-test")
    assert status == "stale"
    assert job.table.dataframe.count() == 1
