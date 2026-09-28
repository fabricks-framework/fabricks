import json
from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor


def _processor(job_response, incoming_edges):
    processor = DagProcessor.__new__(DagProcessor)
    processor.notebook = False
    processor.schedule_id = "sched-1"
    processor.schedule = "daily"
    processor.step = MagicMock(__str__=lambda self: "silver")

    fake_queue = MagicMock()
    fake_queue.sentinel = object()
    fake_queue.receive.side_effect = [json.dumps(job_response), fake_queue.sentinel]

    fake_table = MagicMock()
    fake_table.query.return_value = incoming_edges

    processor.get_azure_queue = MagicMock()
    processor.get_azure_queue.return_value.__enter__.return_value = fake_queue
    processor.get_azure_table = MagicMock()
    processor.get_azure_table.return_value.__enter__.return_value = fake_table

    return processor, fake_table


def test_receive_skips_dispatch_when_every_dependency_is_not_ok():
    job_response = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    incoming_edges = [{"PartitionKey": "dependencies", "JobId": "job-1", "ParentId": "parent-1", "Status": "stale"}]
    processor, _fake_table = _processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = True

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run") as fake_run,
    ):
        processor.receive()

    fake_run.assert_not_called()


def test_receive_dispatches_when_any_dependency_is_ok():
    job_response = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    incoming_edges = [{"PartitionKey": "dependencies", "JobId": "job-1", "ParentId": "parent-1", "Status": "ok"}]
    processor, _fake_table = _processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = True

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value="ok") as fake_run,
    ):
        processor.receive()

    fake_run.assert_called_once()


def test_receive_deletes_its_own_incoming_edges_after_reading_them_even_when_not_skipped():
    # A job with skip_if_stale False (Bronze/Gold's BaseJob-inherited
    # default, or a Silver job that opted out) never triggers a skip
    # decision, but must still delete its incoming edges -- otherwise the
    # 'dependencies' partition only ever grows, since Task 7 changed
    # outgoing-edge writes from delete to update.
    job_response = {"JobId": "job-1", "Job": "gold.fact_dummy"}
    incoming_edges = [{"PartitionKey": "dependencies", "JobId": "job-1", "ParentId": "parent-1", "Status": "ok"}]
    processor, fake_table = _processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = False

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value="ok"),
    ):
        processor.receive()

    fake_table.delete.assert_any_call(incoming_edges)
