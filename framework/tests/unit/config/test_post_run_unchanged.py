from unittest.mock import MagicMock

import pytest

from fabricks.core.jobs.base.checker import JobChecker
from fabricks.core.jobs.base.exception import UnchangedWarning


def _checker(last_version: int, metrics: list[dict | None]) -> JobChecker:
    job = MagicMock()
    job.table.get_last_version.return_value = last_version
    job.table.get_history.return_value.select.return_value.collect.return_value = [
        {"operationMetrics": m} for m in metrics
    ]
    return JobChecker(job)


def test_no_new_version_is_not_judged():
    # e.g. register mode: for_each_run never writes, so there's nothing to compare
    _checker(last_version=3, metrics=[]).post_run_unchanged(3)


def test_new_version_without_affected_rows_raises():
    with pytest.raises(UnchangedWarning):
        _checker(
            last_version=4, metrics=[{"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0"}]
        ).post_run_unchanged(3)


def test_new_version_with_affected_rows_passes():
    _checker(last_version=4, metrics=[{"numTargetRowsInserted": "2"}]).post_run_unchanged(3)
