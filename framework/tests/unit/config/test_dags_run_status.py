from dataclasses import dataclass
import sys
from unittest.mock import patch

import pytest

from fabricks.core.dags.run import run
from fabricks.core.jobs.base.exception import CheckWarning, PreRunCheckException, SkipWarning, UnchangedWarning
from fabricks.utils.log import LogStatus


@dataclass
class _Job:
    """The slice of a job that dags.run touches: `run` either returns or raises `error`."""

    error: Exception | None = None

    def run(self, **_kwargs) -> None:
        if self.error:
            raise self.error


def _run_with_mocked_dbutils(fake_job):
    # run() imports dbutils lazily from databricks.sdk.runtime (faked by this tier's conftest)
    with patch.object(sys.modules["databricks.sdk.runtime"], "dbutils") as fake_dbutils:
        fake_dbutils.notebook.entry_point.getDbutils.side_effect = Exception("no notebook context in a unit test")
        return run(job=fake_job, schedule_id="sched-1", schedule="daily")


def test_dags_run_returns_ok_on_ordinary_completion():
    fake_job = _Job()

    assert _run_with_mocked_dbutils(fake_job) == "ok"


def test_dags_run_returns_stale_when_job_run_raises_unchanged_warning():
    fake_job = _Job(error=UnchangedWarning("no data"))

    assert _run_with_mocked_dbutils(fake_job) == "stale"


def test_dags_run_reraises_a_real_check_exception():
    fake_job = _Job(error=PreRunCheckException("bad data"))

    with pytest.raises(PreRunCheckException):
        _run_with_mocked_dbutils(fake_job)


def test_dags_run_still_logs_done_when_stale_so_terminate_does_not_report_it_as_failed():
    # DagTerminator.terminate() judges completion by scanning log messages for the literal "done"
    fake_job = _Job(error=UnchangedWarning("no data"))

    with patch("fabricks.core.dags.run.LOGGER") as fake_logger:
        _run_with_mocked_dbutils(fake_job)

    logged_messages = [c.args[0] for c in fake_logger.info.call_args_list]
    assert logged_messages.count(LogStatus.DONE) == 1, "a stale job must log done exactly once"


def test_dags_run_still_logs_skipped_for_a_skip_warning():
    # the logs_pivot/last_schedule views (fabricks/deploy/views.py) derive skipped/warned from these literal messages
    fake_job = _Job(error=SkipWarning("explicitly skipped"))

    with patch("fabricks.core.dags.run.LOGGER") as fake_logger:
        status = _run_with_mocked_dbutils(fake_job)

    assert status == "stale"
    logged_messages = [c.args[0] for c in fake_logger.exception.call_args_list]
    assert logged_messages == [LogStatus.SKIPPED]


def test_dags_run_still_logs_warned_for_a_check_warning():
    fake_job = _Job(error=CheckWarning("check failed a warning rule"))

    with patch("fabricks.core.dags.run.LOGGER") as fake_logger:
        status = _run_with_mocked_dbutils(fake_job)

    assert status == "stale"
    logged_messages = [c.args[0] for c in fake_logger.exception.call_args_list]
    assert logged_messages == [LogStatus.WARNED]


def test_dags_run_does_not_log_skipped_or_warned_for_an_unchanged_warning():
    # a routine "no new data" outcome must stay out of logs_pivot's skipped/warned columns
    fake_job = _Job(error=UnchangedWarning("no data"))

    with patch("fabricks.core.dags.run.LOGGER") as fake_logger:
        status = _run_with_mocked_dbutils(fake_job)

    assert status == "stale"
    assert fake_logger.exception.call_args_list == []
