"""The pre-dispatch skip check, proven against a real Databricks workspace:
a small dedicated schedule (bronze.king_scd1 and the two silver jobs tagged
"skip_unchanged" in runtime/fabricks/schedules/schedule.yml) run once here,
as the second run after the full "test" schedule this directory's
conftest.py already runs once per session. That schedule's `variables.iter: 0`
makes bronze.king_scd1's pre_run notebook write an empty commit, so nothing
changed. The first run's counterpart is test_cdc.py: silver.king_scd1 holding
real rows proves bronze wasn't wrongly flagged unchanged then.

Read back through fabricks.last_schedule's `stale` column, not the schedule's
own Azure Table: DagTerminator.terminate() drops that table as part of the
same standalone() call, whereas fabricks.last_schedule is fed by the log
literals it already flushed. That view (fabricks/deploy/views.py's
create_or_replace_last_schedule_view) is a global "whichever schedule_id has
the most recent start_time" view, not scoped by schedule name -- running the
skip_unchanged schedule here would become the new "last schedule" and break
test_schedule.py's own real-schedule assertions if this ran first.
@pytest.mark.order("last") (see test_init.py for the same pytest-order
pattern used the other direction) forces this test to run after
test_schedule.py has already read last_schedule for the "test" schedule,
regardless of file collection order.
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
