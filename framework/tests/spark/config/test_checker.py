"""Checker (framework/fabricks/core/jobs/base/checker.py): the pass/fail
decision logic behind gold.check_* - min_rows/max_rows/count_must_equal
comparisons and their exact error messages, plus the before/after
run-time-window check. Mirrors tests/spark/databricks/jobs/job1/test_check.py's
test_gold_check_max_rows/min_rows/count_must_equal, which today only prove
this via a real schedule run's error log.

check_post_run_extra() only ever touches Spark through self.spark.sql(...)
.collect()[0][0] (the row count) and, for count_must_equal,
self.spark.read.table(...).count() - both controllable on the fake_spark
this tier's conftest already installs, so the comparison/message logic
runs for real with no actual data or container.
"""

from datetime import datetime, timedelta

import pytest

from fabricks.context import TIMEZONE
from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PostRunCheckException, SkipRunTimeWarning
from fabricks.models.job import CheckOptions


def _job_with_check_options(**check_options):
    job = get_job(step="semantic", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(update={"check_options": CheckOptions(**check_options)})
    return job


def _with_row_count(job, count: int):
    job.spark.sql.return_value.collect.return_value = [[count]]
    return job


def test_check_post_run_extra_raises_on_max_rows_exceeded():
    job = _with_row_count(_job_with_check_options(max_rows=2), count=3)

    with pytest.raises(PostRunCheckException, match=r"max rows check failed \(3 > 2\)"):
        job.check_post_run_extra()


def test_check_post_run_extra_raises_on_min_rows_not_met():
    job = _with_row_count(_job_with_check_options(min_rows=2), count=1)

    with pytest.raises(PostRunCheckException, match=r"min rows check failed \(1 < 2\)"):
        job.check_post_run_extra()


def test_check_post_run_extra_passes_when_within_bounds():
    job = _with_row_count(_job_with_check_options(min_rows=1, max_rows=5), count=3)

    job.check_post_run_extra()  # must not raise


def test_check_post_run_extra_skips_entirely_when_no_check_options_set():
    job = _with_row_count(_job_with_check_options(), count=0)
    # job.spark is a lazily-built property (Configurator.spark) that issues
    # a couple of `set ...` calls of its own the first time it's accessed
    # (add_spark_options_to_spark) - force that init now, then reset the
    # call history, so the assertion below is only about what
    # check_post_run_extra() itself does.
    job.spark.sql.reset_mock()

    job.check_post_run_extra()  # must not raise - and must not even query the count

    job.spark.sql.assert_not_called()


def test_check_post_run_extra_raises_on_count_must_equal_mismatch():
    job = _with_row_count(_job_with_check_options(count_must_equal="fabricks.dummy"), count=2)
    job.spark.read.table.return_value.count.return_value = 1

    with pytest.raises(PostRunCheckException, match=r"count must equal check failed \(fabricks\.dummy - 2 != 1\)"):
        job.check_post_run_extra()


def test_check_run_time_before_raises_once_past_the_deadline():
    job = get_job(step="semantic", topic="fact", item="step_option")
    past = (datetime.now(tz=TIMEZONE) - timedelta(hours=1)).strftime("%H:%M:%S")

    with pytest.raises(SkipRunTimeWarning):
        job._check_run_time(past, "before")


def test_check_run_time_before_does_not_raise_ahead_of_the_deadline():
    job = get_job(step="semantic", topic="fact", item="step_option")
    future = (datetime.now(tz=TIMEZONE) + timedelta(hours=1)).strftime("%H:%M:%S")

    job._check_run_time(future, "before")  # must not raise


def test_check_run_time_after_raises_before_the_target_time():
    job = get_job(step="semantic", topic="fact", item="step_option")
    future = (datetime.now(tz=TIMEZONE) + timedelta(hours=1)).strftime("%H:%M:%S")

    with pytest.raises(SkipRunTimeWarning):
        job._check_run_time(future, "after")
