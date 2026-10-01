import pytest

from fabricks.context import DBUTILS, SPARK
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from tests.semblance.fixture import Semblance
from tests.unit.config import conftest as config_conftest


@pytest.fixture(autouse=True)
def _fake_dags_log_table():
    """Override the conftest fixture of the same name: `semblance` must be the only thing patching the
    handler, and the last test inspects it untouched. Drain the buffer so logging.shutdown() has nothing to flush."""
    yield
    TABLE_LOG_HANDLER.clear_buffer()


def test_semblance_and_shared_mocks_do_not_leak_across_tests(semblance, tmp_path):
    SPARK.sql.return_value = 42  # the shared bootstrap mocks: reset, not replaced, between tests
    DBUTILS.credentials.getServiceCredentialsProvider.return_value = "leak"  # ty: ignore[invalid-assignment]
    LOGGER.info(
        "start",
        extra={
            "partition_key": "s1",
            "schedule_id": "s1",
            "schedule": "daily",
            "step": "silver",
            "job": "j",
            "target": "table",
        },
    )
    assert [r["Message"] for r in semblance.table("dags").rows(PartitionKey="s1")] == ["start"]

    # what the autouse reset fixture runs between tests, applied now so one test proves the whole contract
    config_conftest._reset_bootstrap_mocks()
    fresh = Semblance(tmp_path)

    assert SPARK.sql.return_value != 42
    assert DBUTILS.credentials.getServiceCredentialsProvider.return_value != "leak"
    assert "dags" not in fresh.tables.tables, "a new Semblance must start with no rows from earlier ones"


def test_handler_table_is_unresolved_outside_the_fixture():
    assert TABLE_LOG_HANDLER._table is None
