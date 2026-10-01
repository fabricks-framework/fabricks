"""JobChecker.post_run_unchanged: a new Delta version only counts as a change if its operation metrics show
affected rows (a MERGE/WRITE bumps the version even with zero net changes)."""

from dataclasses import dataclass, field

import pytest

from fabricks.core.jobs.base.checker import JobChecker
from fabricks.core.jobs.base.exception import UnchangedWarning


@dataclass
class _History:
    metrics: list[dict | None]

    def select(self, _column: str) -> "_History":
        return self

    def collect(self) -> list[dict]:
        return [{"operationMetrics": m} for m in self.metrics]


@dataclass
class _Table:
    last_version: int
    metrics: list[dict | None]
    history_limits: list[int] = field(default_factory=list)

    def get_last_version(self) -> int:
        return self.last_version

    def get_history(self, limit: int) -> _History:
        self.history_limits.append(limit)
        return _History(self.metrics)


@dataclass
class _Job:
    table: _Table


def _checker(last_version: int, metrics: list[dict | None]) -> tuple[JobChecker, _Table]:
    table = _Table(last_version, metrics)
    return JobChecker(_Job(table)), table


def test_no_new_version_is_not_judged():
    # e.g. register mode: for_each_run never writes, so there's nothing to compare
    checker, table = _checker(last_version=3, metrics=[])

    checker.post_run_unchanged(3)

    assert table.history_limits == [], "an unchanged version must not even read the history"


@pytest.mark.parametrize(
    "metrics",
    [
        pytest.param([{"numTargetRowsInserted": "0", "numTargetRowsUpdated": "0"}], id="all-zero"),
        pytest.param([None], id="no-metrics"),
        pytest.param([{}], id="empty-metrics"),
        pytest.param([{"numSomethingElse": "5"}], id="unrelated-metric-only"),
    ],
)
def test_new_version_without_affected_rows_raises(metrics):
    checker, _ = _checker(last_version=4, metrics=metrics)

    with pytest.raises(UnchangedWarning, match="no data"):
        checker.post_run_unchanged(3)


@pytest.mark.parametrize(
    "metric", ["numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted", "numOutputRows"]
)
def test_new_version_with_any_affected_row_metric_passes(metric):
    checker, _ = _checker(last_version=4, metrics=[{metric: "2"}])

    checker.post_run_unchanged(3)


def test_every_commit_since_the_run_started_is_inspected():
    checker, table = _checker(last_version=6, metrics=[{"numOutputRows": "0"}, None, {"numTargetRowsInserted": "1"}])

    checker.post_run_unchanged(3)

    assert table.history_limits == [3], "versions 4, 5 and 6 were written during the run"
