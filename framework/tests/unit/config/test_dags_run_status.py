from unittest.mock import MagicMock, patch

import pytest

from fabricks.core.dags.run import run
from fabricks.core.jobs.base.exception import PreRunCheckException, UnchangedWarning


def _run_with_mocked_dbutils(fake_job):
    with patch("fabricks.core.dags.run.dbutils") as fake_dbutils:
        fake_dbutils.jobs.taskValues.get.return_value = "sched-1"
        fake_dbutils.notebook.entry_point.getDbutils.side_effect = Exception("no notebook context in a unit test")
        return run(job=fake_job, schedule_id="sched-1", schedule="daily")


def test_dags_run_returns_ok_on_ordinary_completion():
    fake_job = MagicMock()
    fake_job.run.return_value = None

    assert _run_with_mocked_dbutils(fake_job) == "ok"


def test_dags_run_returns_stale_when_job_run_raises_unchanged_warning():
    fake_job = MagicMock()
    fake_job.run.side_effect = UnchangedWarning("no data")

    assert _run_with_mocked_dbutils(fake_job) == "stale"


def test_dags_run_reraises_a_real_check_exception():
    fake_job = MagicMock()
    fake_job.run.side_effect = PreRunCheckException("bad data")

    with pytest.raises(PreRunCheckException):
        _run_with_mocked_dbutils(fake_job)


def test_dags_run_still_logs_done_when_stale_so_terminate_does_not_report_it_as_failed():
    # DagTerminator.terminate() decides "did this job ever complete
    # successfully" purely by scanning log messages for the literal string
    # "done" -- a stale (no new data) outcome is not a failure from its
    # point of view, and must not be silently reported as one.
    fake_job = MagicMock()
    fake_job.run.side_effect = UnchangedWarning("no data")

    with patch("fabricks.core.dags.run.LOGGER") as fake_logger:
        _run_with_mocked_dbutils(fake_job)

    logged_messages = [c.args[0] for c in fake_logger.info.call_args_list]
    assert "done" in logged_messages
