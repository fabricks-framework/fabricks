"""Local schedule tests: real DagProcessor / logging against semblance's fake Azure -- no live service."""

import json

from fabricks.core.dags.log import LOGGER
from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import status_row

SCHEDULE_ID = "s1"


def test_send_dispatches_a_job_without_pending_dependencies_then_a_sentinel_per_worker(semblance):
    semblance.queue(f"qsilver{SCHEDULE_ID}").create()
    semblance.table(f"t{SCHEDULE_ID}").seed([status_row("job-1", job="silver.fact_dummy", rank=1)])

    with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
        processor.send()
        workers = processor.step.workers

    sent = semblance.queue(f"qsilver{SCHEDULE_ID}").sent
    dispatched = json.loads(sent[0])
    assert dispatched["JobId"] == "job-1"
    assert dispatched["Status"] == "waiting"
    assert sent[1:] == ["SENTINEL"] * workers
    assert semblance.table(f"t{SCHEDULE_ID}").rows(PartitionKey="statuses", JobId="job-1")[0]["Status"] == "waiting"


def test_log_records_written_with_target_table_reach_the_dags_table(semblance):
    LOGGER.info(
        "start",
        extra={
            "partition_key": SCHEDULE_ID,
            "schedule_id": SCHEDULE_ID,
            "schedule": "daily",
            "step": "silver",
            "job": "silver.fact_dummy",
            "target": "table",
        },
    )

    rows = semblance.table("dags").rows(PartitionKey=SCHEDULE_ID)
    assert [r["Message"] for r in rows] == ["start"]
    assert rows[0]["Job"] == "silver.fact_dummy"
