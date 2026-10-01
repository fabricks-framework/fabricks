"""JobResolver.spark (fabricks/core/jobs/base/resolver.py) derives an isolated session via newSession() only when a
tier (runtime -> step -> job) configures spark_options, never mutating the parent's.

Fixture spark_options: tests/spark/runtime/fabricks/conf.fabricks.yml (step "semantic") and
tests/spark/runtime/semantic/fact/_config.semantic.yml (job "zstd").
"""

from fabricks.core import get_job


def test_job_without_spark_options_uses_the_runtime_session_directly(local_spark):
    job = get_job(step="gold", topic="fact", item="step_option")

    assert job.spark is local_spark


def test_jobs_in_the_same_step_share_one_derived_session(local_spark):
    table = get_job(step="semantic", topic="fact", item="table")
    step_option = get_job(step="semantic", topic="fact", item="step_option")

    assert table.spark is not local_spark, "a step with spark_options must get its own derived session"
    assert step_option.spark is table.spark, "jobs in the same step should share one derived session"
    assert table.spark.conf.get("fabricks.test.step_marker") == "semantic_step"


def test_a_different_step_does_not_share_another_steps_session(local_spark):
    gold_job = get_job(step="gold", topic="fact", item="step_option")
    semantic_job = get_job(step="semantic", topic="fact", item="table")

    assert gold_job.spark is local_spark
    assert gold_job.spark is not semantic_job.spark


def test_job_level_spark_options_extend_the_step_session_without_leaking_to_siblings(local_spark):
    zstd = get_job(step="semantic", topic="fact", item="zstd")
    table = get_job(step="semantic", topic="fact", item="table")

    assert zstd.spark is not table.spark, "a job with its own spark_options gets its own session, not the step's"
    assert zstd.spark.conf.get("fabricks.test.step_marker") == "semantic_step", (
        "a job session must extend (inherit) its step's conf, not start from a blank session"
    )
    assert zstd.spark.conf.get("spark.sql.parquet.compression.codec") == "zstd"
    assert table.spark.conf.get("spark.sql.parquet.compression.codec") == "snappy", (
        "the zstd job's own conf must not leak back into a sibling job with no job-level spark_options"
    )
