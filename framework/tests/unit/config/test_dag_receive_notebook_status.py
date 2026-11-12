"""A notebook-mode job's exit value becomes its edge status, which a skip_if_stale child compares to "ok"."""

import json
from unittest.mock import MagicMock, patch

import pytest

from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
PARENT, CHILD = "job-parent", "job-child"


def _receive(
    semblance, *, step: str, job_id: str, job_name: str, skip_if_stale: bool, notebook: bool, exit_value=None
):
    queue = semblance.queue(f"q{step}{SCHEDULE_ID}")
    queue.create()
    queue.send(json.dumps(status_row(job_id, status="waiting", job=job_name, schedule_id=SCHEDULE_ID)))
    queue.send("SENTINEL")

    dbutils = MagicMock()
    if isinstance(exit_value, Exception):
        dbutils.notebook.run.side_effect = exit_value
    else:
        dbutils.notebook.run.return_value = exit_value
    with (
        patch("fabricks.core.dags.processor.get_job", return_value=MagicMock(skip_if_stale=skip_if_stale)),
        patch("fabricks.core.dags.processor.run", return_value="ok"),
        patch("databricks.sdk.runtime.dbutils", dbutils, create=True),
        DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step=step, notebook=notebook) as processor,
    ):
        processor.receive()


def _status(semblance, job_id: str) -> str:
    return semblance.table(TABLE).rows(PartitionKey="statuses", JobId=job_id)[0]["Status"]


@pytest.mark.parametrize(
    ("exit_value", "child_runs"),
    [
        pytest.param("ok", True, id="ok"),
        pytest.param("stale", False, id="stale"),
        # the runtime repo's own run notebook ignores run()'s result and always exits with this
        pytest.param("exit (0)", True, id="runtime-run-notebook-exit"),
        pytest.param("ok\n", True, id="trailing-newline"),
        pytest.param("'ok'", True, id="quoted"),
        pytest.param(None, True, id="no-exit-value"),
        pytest.param(RuntimeError("notebook failed"), False, id="notebook-raises"),
    ],
)
def test_a_child_skips_only_when_its_notebook_parent_exits_exactly_stale(semblance, exit_value, child_runs):
    semblance.table(TABLE).seed(
        [
            status_row(PARENT, status="scheduled", job="bronze.parent", schedule_id=SCHEDULE_ID),
            status_row(CHILD, status="scheduled", job="silver.child", schedule_id=SCHEDULE_ID),
            dependency_row(CHILD, PARENT, status="pending", schedule_id=SCHEDULE_ID),
        ]
    )

    _receive(
        semblance,
        step="bronze",
        job_id=PARENT,
        job_name="bronze.parent",
        skip_if_stale=False,
        notebook=True,
        exit_value=exit_value,
    )
    _receive(semblance, step="silver", job_id=CHILD, job_name="silver.child", skip_if_stale=True, notebook=False)

    # the fake run() returns "ok", so a child that ran is "ok" and a skipped one is "stale"
    assert _status(semblance, CHILD) == ("ok" if child_runs else "stale")
