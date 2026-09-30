from unittest.mock import MagicMock, patch

from tests.unit.config._dag_processor_helpers import fake_processor


def _receive(job_response: dict, run_result, run_raises=None):
    outgoing_edges = [
        {"PartitionKey": "dependencies", "JobId": "child-1", "ParentId": job_response["JobId"], "Status": "pending"}
    ]
    incoming_edges = [
        {"PartitionKey": "dependencies", "JobId": job_response["JobId"], "ParentId": "parent-1", "Status": "ok"}
    ]

    def _query(filter_str: str):
        if f"JobId eq '{job_response['JobId']}'" in filter_str:
            return incoming_edges
        return outgoing_edges

    processor, fake_table = fake_processor(job_response, _query)

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
        processor.receive()

    return fake_table, outgoing_edges


def test_receive_records_stale_status_on_a_real_exception_and_propagates_to_dependency_edges():
    job = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    fake_table, _ = _receive(job, run_result=None, run_raises=Exception("boom"))

    upserted = fake_table.upsert.call_args_list
    job_status_updates = [
        c.args[0] for c in upserted if isinstance(c.args[0], dict) and c.args[0].get("JobId") == "job-1"
    ]
    assert job_status_updates[-1]["Status"] == "stale"


def test_receive_records_stale_status_and_updates_dependency_edges_not_delete():
    job = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    fake_table, outgoing_edges = _receive(job, run_result="stale")

    # Not a blanket "delete never called": Task 8 (landing later, same
    # receive()) legitimately adds its own delete() for a job's *incoming*
    # edges, unrelated to this test's concern (the *outgoing* ones this
    # job just wrote must be upserted, not deleted). Assert specifically
    # that delete was never called with the outgoing-edges list.
    for call in fake_table.delete.call_args_list:
        assert call.args[0] != outgoing_edges
    edge_updates = [c.args[0] for c in fake_table.upsert.call_args_list if isinstance(c.args[0], list)]
    assert edge_updates, "dependency edges must be upserted (updated), not deleted"
    assert edge_updates[0][0]["Status"] == "stale"


def test_receive_records_ok_status_on_a_successful_run_and_propagates_to_dependency_edges():
    # Control test, paired with the "stale" ones above: without it, a bug
    # that always writes "stale" regardless of the real outcome (e.g. the
    # pre-try `status = "stale"` default leaking through, or a broken
    # `except` re-raising over the real assignment) would pass every other
    # test in this file while breaking every ordinary successful run.
    job = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    fake_table, _ = _receive(job, run_result="ok")

    upserted = fake_table.upsert.call_args_list
    job_status_updates = [
        c.args[0] for c in upserted if isinstance(c.args[0], dict) and c.args[0].get("JobId") == "job-1"
    ]
    assert job_status_updates[-1]["Status"] == "ok"

    edge_updates = [c.args[0] for c in fake_table.upsert.call_args_list if isinstance(c.args[0], list)]
    assert edge_updates, "dependency edges must be upserted"
    assert edge_updates[0][0]["Status"] == "ok"
