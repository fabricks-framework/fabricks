from unittest.mock import MagicMock, patch

import pytest

from fabricks.core import get_job
from fabricks.core.jobs.base.exception import PreRunCheckException, UnchangedWarning


def _stubbed_silver_job(monkeypatch):
    # job.run()'s exception handling around for_each_run is the one thing
    # under test here -- checks/invokers/restore are all no-ops so a new
    # method added to either later doesn't silently need its own line here.
    job = get_job(step="silver", topic="append_test", item="test")

    monkeypatch.setattr(job, "_checker", MagicMock())
    monkeypatch.setattr(job, "_invoker", MagicMock())
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
