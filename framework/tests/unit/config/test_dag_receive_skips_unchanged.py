from unittest.mock import MagicMock, patch

import pytest

from fabricks.utils.log import LogStatus
from tests.unit.config._dag_processor_helpers import fake_processor


@pytest.fixture(autouse=True)
def _no_real_log_table():
    # receive() logs to the table (target="table") and flushes it: the real handler would resolve a real Azure table
    with patch("fabricks.core.dags.processor.LOGGER"), patch("fabricks.core.dags.processor.TABLE_LOG_HANDLER"):
        yield


def test_receive_skips_dispatch_when_every_dependency_is_not_ok():
    job_response = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    incoming_edges = [{"PartitionKey": "dependencies", "JobId": "job-1", "ParentId": "parent-1", "Status": "stale"}]
    processor, _fake_table = fake_processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = True

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run") as fake_run,
        patch("fabricks.core.dags.processor.LOGGER") as fake_logger,
    ):
        processor.receive()

    fake_run.assert_not_called()
    logged = [call.args[0] for call in fake_logger.info.call_args_list]
    assert LogStatus.DONE in logged
    assert LogStatus.STALE in logged


def test_receive_dispatches_when_any_dependency_is_ok():
    job_response = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    incoming_edges = [{"PartitionKey": "dependencies", "JobId": "job-1", "ParentId": "parent-1", "Status": "ok"}]
    processor, _fake_table = fake_processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = True

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value="ok") as fake_run,
    ):
        processor.receive()

    fake_run.assert_called_once()


def test_receive_dispatches_when_there_are_no_dependencies_at_all():
    # The vacuous-truth guard: any(...) over an empty list is False, so
    # `not any(...)` on zero incoming edges is vacuously True -- without
    # the explicit `and incoming` guard in the skip predicate, a job with
    # no dependencies at all (e.g. a root Silver job) would incorrectly be
    # treated as "none of my dependencies are ok" and skipped forever.
    job_response = {"JobId": "job-1", "Job": "silver.fact_dummy"}
    processor, _fake_table = fake_processor(job_response, [])

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
    processor, fake_table = fake_processor(job_response, incoming_edges)

    fake_job = MagicMock()
    fake_job.skip_if_stale = False

    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value="ok"),
    ):
        processor.receive()

    fake_table.delete.assert_any_call(incoming_edges)
