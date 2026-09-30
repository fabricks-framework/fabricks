import json
from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor
from fabricks.utils.log import LogStatus
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
QUEUE = f"qsilver{SCHEDULE_ID}"


def _receive(semblance, incoming_status: str | None, *, skip_if_stale: bool, run_result="ok", patch_logger=False):
    job = status_row("job-1", status="waiting", job="silver.fact_dummy", schedule_id=SCHEDULE_ID)
    rows = [job]
    if incoming_status is not None:
        rows.append(dependency_row("job-1", "parent-1", status=incoming_status, schedule_id=SCHEDULE_ID))
    semblance.table(TABLE).seed(rows)
    semblance.queue(QUEUE).create()
    semblance.queue(QUEUE).send(json.dumps(job))
    semblance.queue(QUEUE).send("SENTINEL")

    fake_job = MagicMock()
    fake_job.skip_if_stale = skip_if_stale
    patches = [
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value=run_result),
    ]
    if patch_logger:
        patches.append(patch("fabricks.core.dags.processor.LOGGER"))

    mocks = [p.start() for p in patches]
    try:
        with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
            processor.receive()
    finally:
        for p in patches:
            p.stop()
    return mocks  # [get_job, run, (LOGGER)]


def test_receive_skips_dispatch_when_every_dependency_is_not_ok(semblance):
    _get_job, fake_run, fake_logger = _receive(semblance, "stale", skip_if_stale=True, patch_logger=True)

    fake_run.assert_not_called()
    logged = [call.args[0] for call in fake_logger.info.call_args_list]
    assert LogStatus.DONE in logged
    assert LogStatus.STALE in logged


def test_receive_dispatches_when_any_dependency_is_ok(semblance):
    _get_job, fake_run = _receive(semblance, "ok", skip_if_stale=True)

    fake_run.assert_called_once()


def test_receive_dispatches_when_there_are_no_dependencies_at_all(semblance):
    # The vacuous-truth guard: any(...) over an empty list is False, so `not any(...)` on zero
    # incoming edges is vacuously True -- without the explicit `and incoming` guard, a root job
    # would be treated as "none of my dependencies are ok" and skipped forever.
    _get_job, fake_run = _receive(semblance, None, skip_if_stale=True)

    fake_run.assert_called_once()


def test_receive_deletes_its_own_incoming_edges_after_reading_them_even_when_not_skipped(semblance):
    # skip_if_stale False never triggers a skip decision but must still delete incoming edges,
    # otherwise the 'dependencies' partition only ever grows.
    _receive(semblance, "ok", skip_if_stale=False)

    assert semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="job-1") == []
