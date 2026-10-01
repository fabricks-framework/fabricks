"""The pre-dispatch skip check against a real workspace: a dedicated schedule (bronze.king_scd1 and the
two silver jobs tagged "skip_unchanged" in runtime/fabricks/schedules/schedule.yml) runs once, after the
full "test" schedule that this directory's conftest.py runs per session. `variables.iter: 0` makes
bronze.king_scd1's pre_run notebook write an empty commit, so nothing changed. test_cdc.py is the
counterpart: silver.king_scd1 holds real rows, so bronze was not wrongly flagged unchanged.

Read through fabricks.last_schedule's `stale` column, not the schedule's Azure Table (dropped by
DagTerminator.terminate()). That view is global ("most recent schedule_id"), so running this schedule
first would break test_schedule.py; `@pytest.mark.order("last")` forces it to run after.
"""

import pytest

from fabricks.context import SPARK
from fabricks.core.schedules import standalone


def _status(job: str):
    rows = SPARK.sql(f"select stale, failed from fabricks.last_schedule where job = '{job}'").collect()
    assert len(rows) == 1, f"expected exactly one {job} last_schedule row"
    return rows[0]


@pytest.mark.order("last")
def test_second_schedule_run_skips_silver_when_bronze_is_unchanged():
    standalone(schedule="skip_unchanged")

    # bronze ran and was judged unchanged; both silver jobs (king_and_queen's
    # other parent, bronze.queen_scd1, isn't in this schedule) were skipped
    # before dispatch -- stale, and not counted as failed.
    for job in ("bronze.king_scd1", "silver.king_scd1", "silver.king_and_queen_scd1"):
        status = _status(job)
        assert status.stale, f"{job} must be stale: bronze.king_scd1 didn't change between the two runs"
        assert not status.failed, f"{job} must not be failed"
