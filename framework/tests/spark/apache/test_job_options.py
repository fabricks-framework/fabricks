"""Real get_job()-driven job-orchestration behaviors that don't fit
test_cdc.py (CDC-class-level) or test_feature.py (DDL-level) -- one test per
feature, matching tests/spark/databricks/test_schedule.py's convention.
Each job here is called directly through BaseJob.for_each_batch()/similar
real methods (not a full scheduled run) since these behaviors don't depend
on bronze/dependency plumbing.
"""

from fabricks.core import get_job


def test_append_mode_accumulates_across_batches(local_spark):
    job = get_job(step="silver", topic="append_test", item="test")

    job.for_each_batch(local_spark.createDataFrame([(1, "a")], ["id", "name"]))
    assert job.table.dataframe.count() == 1

    job.for_each_batch(local_spark.createDataFrame([(2, "b")], ["id", "name"]))
    assert job.table.dataframe.count() == 2


# "latest" mode's default (dedup on) always hits NoCDC's qualify-based dedup
# CTE (see test_cdc.py's test_order_duplicate_by/test_deduplicate comment),
# which OSS Spark's parser rejects but Databricks' does not -- this job sets
# the real, supported deduplicate:false option to sidestep that, valid here
# since neither batch below has duplicate keys within itself.
def test_latest_mode_replaces_target_with_each_batchs_full_snapshot(local_spark):
    job = get_job(step="silver", topic="latest_test", item="test")
    columns = ["id", "name", "__operation", "__timestamp"]

    job.for_each_batch(local_spark.createDataFrame([(1, "a", "reload", "2022-01-01 00:00:00")], columns))
    assert job.table.dataframe.count() == 1

    # "latest" mode is a full replace (NoCDC.complete() -> insert overwrite),
    # not an incremental merge -- it's up to the caller to feed a complete
    # current-state snapshot each batch, same as a real parser would.
    job.for_each_batch(
        local_spark.createDataFrame(
            [(1, "a", "reload", "2022-01-02 00:00:00"), (2, "b", "reload", "2022-01-02 00:00:00")], columns
        )
    )
    assert job.table.dataframe.count() == 2
