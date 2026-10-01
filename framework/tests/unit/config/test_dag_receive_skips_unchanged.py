import pytest

from fabricks.utils.log import LogStatus
from tests.unit.config._dag import SCHEDULE_ID, edge, incoming_edges, job_status, receive


def test_receive_skips_dispatch_when_every_dependency_is_not_ok(semblance):
    receive(semblance, incoming=["stale", "stale"], skip_if_stale=True, run_result="ok")

    assert job_status(semblance) == "stale", "a dispatched job would have been recorded as ok"
    assert edge(semblance, "child-1")["Status"] == "stale", "the skip must propagate to the outgoing edges"
    assert incoming_edges(semblance) == []


def test_receive_logs_done_and_stale_for_a_skipped_job(semblance):
    # DagTerminator only counts a job as not failed if it sees DONE; last_schedule.stale reads STALE.
    receive(semblance, incoming=["stale"], skip_if_stale=True)

    messages = [r["Message"] for r in semblance.table("dags").rows(PartitionKey=SCHEDULE_ID)]
    assert LogStatus.DONE in messages
    assert LogStatus.STALE in messages


@pytest.mark.parametrize(
    ("incoming", "skip_if_stale"),
    [
        pytest.param(["ok"], True, id="ok-dependency"),
        pytest.param(["stale", "ok"], True, id="any-ok-dependency-is-enough"),
        pytest.param(["stale", "stale"], False, id="skip-if-stale-off"),
        # The vacuous-truth guard: `not any(...)` over zero incoming edges is True, so without the explicit
        # `and incoming` a root job would be treated as "none of my dependencies are ok" and skipped forever.
        pytest.param([], True, id="no-dependencies"),
    ],
)
def test_receive_dispatches_unless_every_dependency_is_stale_and_skip_if_stale_is_on(
    semblance, incoming, skip_if_stale
):
    receive(semblance, incoming=incoming, skip_if_stale=skip_if_stale, run_result="ok")

    assert job_status(semblance) == "ok"
    assert incoming_edges(semblance) == [], "incoming edges are deleted even when the job is not skipped"
