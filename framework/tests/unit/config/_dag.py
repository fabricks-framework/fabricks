"""Drive one real `DagProcessor.receive()` pass against semblance's fake Azure queue and table.

Import this module at the top of the test file: the `semblance` fixture only patches the DAG modules a test
has already imported."""

from collections.abc import Sequence
from dataclasses import dataclass
import json
from unittest.mock import patch

from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
QUEUE = f"qsilver{SCHEDULE_ID}"
JOB_ID = "job-1"


@dataclass
class _Job:
    skip_if_stale: bool


def receive(
    semblance,
    *,
    incoming: Sequence[str] = (),
    outgoing: bool = True,
    skip_if_stale: bool = False,
    run_result: str | None = "ok",
    run_raises: Exception | None = None,
) -> None:
    """`incoming` is the Status of each edge this job waits on; `outgoing` seeds a pending child edge."""
    job = status_row(JOB_ID, status="waiting", job="silver.fact_dummy", schedule_id=SCHEDULE_ID)
    rows = [job]
    rows += [
        dependency_row(JOB_ID, f"parent-{i}", status=status, schedule_id=SCHEDULE_ID)
        for i, status in enumerate(incoming)
    ]
    if outgoing:
        rows.append(dependency_row("child-1", JOB_ID, status="pending", schedule_id=SCHEDULE_ID))
        rows.append(dependency_row("child-2", "unrelated-parent", status="pending", schedule_id=SCHEDULE_ID))
    semblance.table(TABLE).seed(rows)
    semblance.queue(QUEUE).create()
    semblance.queue(QUEUE).send(json.dumps(job))
    semblance.queue(QUEUE).send("SENTINEL")

    def _run(*_args, **_kwargs):
        if run_raises:
            raise run_raises
        return run_result

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=_Job(skip_if_stale)),
        patch("fabricks.core.dags.processor.run", side_effect=_run),
        DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor,
    ):
        processor.receive()


def job_status(semblance) -> str:
    return semblance.table(TABLE).rows(PartitionKey="statuses", JobId=JOB_ID)[0]["Status"]


def edge(semblance, child: str) -> dict:
    return semblance.table(TABLE).rows(PartitionKey="dependencies", JobId=child)[0]


def incoming_edges(semblance) -> list[dict]:
    return semblance.table(TABLE).rows(PartitionKey="dependencies", JobId=JOB_ID)
