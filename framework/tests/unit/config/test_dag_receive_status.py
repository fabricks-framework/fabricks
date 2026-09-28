from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor


def _processor_with_mocked_queue_and_table(job_response: dict, run_result, run_raises=None):
    processor = DagProcessor.__new__(DagProcessor)  # bypass __init__'s real Azure client setup
    processor.notebook = False
    processor.schedule_id = "sched-1"
    processor.schedule = "daily"
    processor.step = MagicMock(__str__=lambda self: "silver")

    fake_queue = MagicMock()
    fake_queue.sentinel = object()
    fake_queue.receive.side_effect = [__import__("json").dumps(job_response), fake_queue.sentinel]

    fake_table = MagicMock()
    fake_table.query.return_value = [
        {"PartitionKey": "dependencies", "JobId": "child-1", "ParentId": job_response["JobId"], "Status": "pending"}
    ]

    processor.get_azure_queue = MagicMock()
    processor.get_azure_queue.return_value.__enter__.return_value = fake_queue
    processor.get_azure_table = MagicMock()
    processor.get_azure_table.return_value.__enter__.return_value = fake_table

    with patch("fabricks.core.dags.processor.run") as fake_run:
        if run_raises:
            fake_run.side_effect = run_raises
        else:
            fake_run.return_value = run_result
        processor.receive()

    return fake_table


def test_receive_records_stale_status_on_a_real_exception_and_propagates_to_dependency_edges():
    job = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    fake_table = _processor_with_mocked_queue_and_table(job, run_result=None, run_raises=Exception("boom"))

    upserted = fake_table.upsert.call_args_list
    job_status_updates = [
        c.args[0] for c in upserted if isinstance(c.args[0], dict) and c.args[0].get("JobId") == "job-1"
    ]
    assert job_status_updates[-1]["Status"] == "stale"


def test_receive_records_stale_status_and_updates_dependency_edges_not_delete():
    job = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    fake_table = _processor_with_mocked_queue_and_table(job, run_result="stale")

    # Not a blanket "delete never called": Task 8 (landing later, same
    # receive()) legitimately adds its own delete() for a job's *incoming*
    # edges, unrelated to this test's concern (the *outgoing* ones this
    # job just wrote must be upserted, not deleted). Assert specifically
    # that delete was never called with the outgoing-edges list.
    outgoing_edges = fake_table.query.return_value
    for call in fake_table.delete.call_args_list:
        assert call.args[0] != outgoing_edges
    edge_updates = [c.args[0] for c in fake_table.upsert.call_args_list if isinstance(c.args[0], list)]
    assert edge_updates, "dependency edges must be upserted (updated), not deleted"
    assert edge_updates[0][0]["Status"] == "stale"
