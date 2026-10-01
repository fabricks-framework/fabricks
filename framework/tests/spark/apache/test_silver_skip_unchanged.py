"""End-to-end proof (real Spark/Delta) that ordinary Silver runs did not regress and that a genuinely
empty batch surfaces as a "stale" RunStatus through the real dags.run.run() wrapper.

run() reads dbutils lazily from databricks.sdk.runtime and flushes the real dags log handler in its
`finally`; both need a fake outside Databricks (no workspace to authenticate against, no Azure storage
account). The `semblance` fixture provides them and restores everything after each test.
"""

from fabricks.core.dags.run import run


def test_dags_run_returns_stale_for_a_genuinely_empty_silver_batch(local_spark, monkeypatch, semblance, fresh_job):
    # append_test's data source isn't wired up in this runtime; get_data() is the seam job.run() sources a batch from.
    job = fresh_job("silver", "append_test", "test")
    first_batch = local_spark.createDataFrame([(1, "a")], ["id", "name"])

    # is_stream defaults True, which would need a "fabricks.dummy" streaming placeholder table this runtime lacks.
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
