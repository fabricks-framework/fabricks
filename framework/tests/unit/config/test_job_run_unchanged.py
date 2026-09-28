from unittest.mock import MagicMock, patch

import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PreRunCheckException, UnchangedWarning


def _stubbed_silver_job(monkeypatch):
    job = get_job(step="silver", topic="append_test", item="test")

    monkeypatch.setattr(job._checker, "run_before", lambda: None)
    monkeypatch.setattr(job._checker, "run_after", lambda: None)
    monkeypatch.setattr(job._checker, "skip_run", lambda: None)
    monkeypatch.setattr(job._checker, "pre_run", lambda: None)
    monkeypatch.setattr(job._checker, "post_run", lambda: None)
    monkeypatch.setattr(job._checker, "post_run_extra", lambda: None)
    monkeypatch.setattr(job._invoker, "pre_run", lambda schedule=None: None)
    monkeypatch.setattr(job._invoker, "post_run", lambda schedule=None: None)
    monkeypatch.setattr(job, "restore", MagicMock())

    return job


def test_run_reraises_unchanged_warning_without_restoring_or_retrying(monkeypatch):
    job = _stubbed_silver_job(monkeypatch)

    with patch.object(job, "for_each_run", side_effect=UnchangedWarning("no data")), pytest.raises(UnchangedWarning):
        job.run(invoke=False, retry=True)

    job.restore.assert_not_called()


def test_run_restores_on_a_real_check_exception(monkeypatch):
    job = _stubbed_silver_job(monkeypatch)

    with (
        patch.object(job, "for_each_run", side_effect=PreRunCheckException("bad data")),
        pytest.raises(PreRunCheckException),
    ):
        job.run(invoke=False)

    job.restore.assert_called_once()
