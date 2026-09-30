import json
from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
QUEUE = f"qsilver{SCHEDULE_ID}"


def _receive(semblance, *, run_result=None, run_raises=None):
    job = status_row("job-1", status="waiting", job="silver.fact_dummy", schedule_id=SCHEDULE_ID)
    semblance.table(TABLE).seed(
        [
            job,
            dependency_row("child-1", "job-1", status="pending", schedule_id=SCHEDULE_ID),  # outgoing edge
            dependency_row("job-1", "parent-1", status="ok", schedule_id=SCHEDULE_ID),  # incoming edge
        ]
    )
    semblance.queue(QUEUE).create()
    semblance.queue(QUEUE).send(json.dumps(job))
    semblance.queue(QUEUE).send("SENTINEL")

    fake_job = MagicMock()
    fake_job.skip_if_stale = False
    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run") as fake_run,
    ):
        if run_raises:
            fake_run.side_effect = run_raises
        else:
            fake_run.return_value = run_result
        with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
            processor.receive()


def _job_status(semblance) -> str:
    return semblance.table(TABLE).rows(PartitionKey="statuses", JobId="job-1")[0]["Status"]


def _outgoing_edge(semblance) -> dict:
    return semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="child-1")[0]


def test_receive_records_stale_status_on_a_real_exception_and_propagates_to_dependency_edges(semblance):
    _receive(semblance, run_raises=Exception("boom"))

    assert _job_status(semblance) == "stale"
    assert _outgoing_edge(semblance)["Status"] == "stale"


def test_receive_records_stale_status_and_updates_dependency_edges_not_delete(semblance):
    _receive(semblance, run_result="stale")

    assert _job_status(semblance) == "stale"
    # the outgoing edge this job just wrote must be updated in place, not deleted
    assert _outgoing_edge(semblance)["Status"] == "stale"


def test_receive_records_ok_status_on_a_successful_run_and_propagates_to_dependency_edges(semblance):
    # Control test, paired with the "stale" ones above: without it, a bug that always writes "stale"
    # regardless of the real outcome would pass every other test in this file.
    _receive(semblance, run_result="ok")

    assert _job_status(semblance) == "ok"
    assert _outgoing_edge(semblance)["Status"] == "ok"


def test_receive_deletes_its_own_incoming_edges(semblance):
    _receive(semblance, run_result="ok")

    assert semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="job-1") == []
