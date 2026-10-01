"""Checker (framework/fabricks/core/jobs/base/checker.py): the pass/fail decision logic behind gold.check_*,
i.e. min_rows/max_rows/count_must_equal comparisons and their exact error messages, the __action/__skip
row decisions, and the before/after run-time window.

Spark is faked: the row count and the check query's rows are the only inputs, so the comparison, the
fail-before-warning precedence and the clock logic run for real."""

import datetime
from types import SimpleNamespace

from pyspark.sql import Row
import pytest

from fabricks.context import TIMEZONE
from fabricks.core import get_job
import fabricks.core.jobs.base.checker as checker_module
from fabricks.core.jobs.base.checker import decide
from fabricks.core.jobs.base.exception import (
    PostRunCheckException,
    PostRunCheckWarning,
    PreRunCheckException,
    PreRunCheckWarning,
    SkipRunCheckWarning,
    SkipRunTimeWarning,
)
from fabricks.models.job import CheckOptions

_QUERIES = {"__action == 'fail'", "__action == 'warning'", "__skip"}


class _RowsDF:
    """The check query's result. `where` really filters, and refuses any predicate the checker should not use."""

    def __init__(self, rows: list[Row]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def where(self, expr: str) -> "_RowsDF":
        assert expr in _QUERIES, f"unexpected predicate {expr!r}"
        self.queries.append(expr)
        if expr == "__skip":
            kept = [r for r in self.rows if r["__skip"]]
        else:
            kept = [r for r in self.rows if r["__action"] == expr.split("'")[1]]
        result = _RowsDF(kept)
        result.queries = self.queries
        return result

    def collect(self) -> list[Row]:
        return self.rows


def _job_with_check_options(**check_options):
    job = get_job(step="gold", topic="fact", item="step_option")
    job.conf = job.conf.model_copy(update={"check_options": CheckOptions(**check_options)})
    return job


def _check_job(rows: list[Row], **check_options):
    # gold.fact.check: no check_options in its own YAML, only exists so
    # check.pre_run.sql/check.skip.sql (Checker.check_pre_run/check_skip_run assert these files exist)
    # have a job to attach to.
    job = get_job(step="gold", topic="fact", item="check")
    job.conf = job.conf.model_copy(update={"check_options": CheckOptions(**check_options)})
    job.spark.sql.return_value = _RowsDF(rows)
    return job


def _with_row_count(job, count: int):
    job.spark.sql.return_value.collect.return_value = [[count]]
    return job


def test_check_post_run_extra_raises_on_max_rows_exceeded():
    job = _with_row_count(_job_with_check_options(max_rows=2), count=3)

    with pytest.raises(PostRunCheckException, match=r"max rows check failed \(3 > 2\)"):
        job._checker.post_run_extra()


def test_check_post_run_extra_raises_on_min_rows_not_met():
    job = _with_row_count(_job_with_check_options(min_rows=2), count=1)

    with pytest.raises(PostRunCheckException, match=r"min rows check failed \(1 < 2\)"):
        job._checker.post_run_extra()


@pytest.mark.parametrize(
    ("options", "count"),
    [
        pytest.param({"min_rows": 1, "max_rows": 5}, 3, id="inside-bounds"),
        pytest.param({"min_rows": 2}, 2, id="count-equals-min"),
        pytest.param({"max_rows": 2}, 2, id="count-equals-max"),
    ],
)
def test_check_post_run_extra_passes_within_bounds(options, count):
    job = _with_row_count(_job_with_check_options(**options), count=count)

    job._checker.post_run_extra()


def test_check_post_run_extra_skips_entirely_when_no_check_options_set():
    job = _with_row_count(_job_with_check_options(), count=0)
    # job.spark is a lazily-built property that issues a couple of `set ...` calls the first time it is
    # accessed; force that now so the assertion below is only about post_run_extra().
    job.spark.sql.reset_mock()

    job._checker.post_run_extra()

    job.spark.sql.assert_not_called()


def test_check_post_run_extra_raises_on_count_must_equal_mismatch():
    job = _with_row_count(_job_with_check_options(count_must_equal="fabricks.dummy"), count=2)
    job.spark.read.table.return_value.count.return_value = 1

    with pytest.raises(PostRunCheckException, match=r"count must equal check failed \(fabricks\.dummy - 2 != 1\)"):
        job._checker.post_run_extra()


def test_check_post_run_extra_passes_when_count_must_equal_matches():
    job = _with_row_count(_job_with_check_options(count_must_equal="fabricks.dummy"), count=2)
    job.spark.read.table.return_value.count.return_value = 2

    job._checker.post_run_extra()


_NOW = datetime.datetime(2026, 1, 15, 12, 0, 0, tzinfo=TIMEZONE)


@pytest.fixture
def frozen_clock(monkeypatch):
    class _Frozen(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return _NOW

    monkeypatch.setattr(checker_module, "datetime", SimpleNamespace(datetime=_Frozen))


@pytest.mark.parametrize(
    ("when", "time", "raises"),
    [
        pytest.param("before", "11:59:59", True, id="before-past-deadline"),
        pytest.param("before", "12:00:00", True, id="before-at-deadline"),
        pytest.param("before", "12:00:01", False, id="before-ahead-of-deadline"),
        pytest.param("after", "11:59:59", False, id="after-target-passed"),
        pytest.param("after", "12:00:00", True, id="after-at-target"),
        pytest.param("after", "12:00:01", True, id="after-before-target"),
    ],
)
def test_check_run_time_window(frozen_clock, when, time, raises):
    job = get_job(step="gold", topic="fact", item="step_option")

    if raises:
        with pytest.raises(SkipRunTimeWarning):
            job._checker._run_time(time, when)
    else:
        job._checker._run_time(time, when)


@pytest.mark.parametrize(
    ("option", "time", "method"), [("before", "11:00:00", "run_before"), ("after", "13:00:00", "run_after")]
)
def test_run_before_and_after_read_the_check_options(frozen_clock, option, time, method):
    job = _job_with_check_options(**{option: time})

    with pytest.raises(SkipRunTimeWarning):
        getattr(job._checker, method)()


def test_run_before_and_after_are_noops_without_check_options(frozen_clock):
    job = _job_with_check_options()

    job._checker.run_before()
    job._checker.run_after()


def test_decide_prefers_fail_over_warning_and_reports_the_last_message():
    fail = [Row(__action="fail", __message="first"), Row(__action="fail", __message="last")]
    warning = [Row(__action="warning", __message="careful")]

    pre = decide("pre_run", fail, warning)
    post = decide("post_run", fail, warning)

    assert isinstance(pre, PreRunCheckException)
    assert str(pre) == "last"
    assert isinstance(post, PostRunCheckException)
    assert str(post) == "last"


def test_decide_returns_none_without_rows():
    assert decide("pre_run", [], []) is None


def test_check_pre_run_raises_on_fail_action():
    job = _check_job([Row(__action="fail", __message="boom")], pre_run=True)

    with pytest.raises(PreRunCheckException, match="boom"):
        job._checker.pre_run()


def test_check_pre_run_raises_warning_when_no_fail_rows():
    job = _check_job([Row(__action="warning", __message="careful")], pre_run=True)

    with pytest.raises(PreRunCheckWarning, match="careful"):
        job._checker.pre_run()


def test_check_pre_run_fail_wins_over_warning_and_skips_the_warning_query():
    df = _RowsDF([Row(__action="warning", __message="careful"), Row(__action="fail", __message="boom")])
    job = _check_job([], pre_run=True)
    job.spark.sql.return_value = df

    with pytest.raises(PreRunCheckException, match="boom"):
        job._checker.pre_run()

    assert df.queries == ["__action == 'fail'"], "a failing check must not spend a second scan on warnings"


def test_check_pre_run_passes_when_no_rows():
    job = _check_job([], pre_run=True)

    job._checker.pre_run()


def test_check_skip_run_raises_when_skip_row_present():
    job = _check_job([Row(__skip=True, __message="skip me")], skip=True)

    with pytest.raises(SkipRunCheckWarning, match="skip me"):
        job._checker.skip_run()


def test_check_skip_run_passes_when_no_skip_rows():
    job = _check_job([Row(__skip=False, __message="not skipped")], skip=True)

    job._checker.skip_run()


# post_run uses the same __action mechanism as pre_run, driven off check.post_run.sql.
def test_check_post_run_raises_on_fail_action():
    job = _check_job([Row(__action="fail", __message="post run boom")], post_run=True)

    with pytest.raises(PostRunCheckException, match="post run boom"):
        job._checker.post_run()


def test_check_post_run_raises_warning_when_no_fail_rows():
    job = _check_job([Row(__action="warning", __message="post run careful")], post_run=True)

    with pytest.raises(PostRunCheckWarning, match="post run careful"):
        job._checker.post_run()


def test_check_post_run_passes_when_no_rows():
    job = _check_job([], post_run=True)

    job._checker.post_run()


def test_check_post_run_raises_when_check_file_not_found():
    # step_option has no `.post_run.sql` fixture, unlike `check`.
    job = _job_with_check_options(post_run=True)

    with pytest.raises(AssertionError, match="post_run check not found"):
        job._checker.post_run()
