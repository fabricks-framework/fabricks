import pytest

from fabricks.context import DBUTILS, SPARK
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


@pytest.fixture(autouse=True)
def _fake_dags_log_table():
    """Override the conftest fixture of the same name: `semblance` must be the only thing patching the
    handler, and the last test inspects it untouched. Drain the buffer so logging.shutdown() has nothing to flush."""
    yield
    TABLE_LOG_HANDLER.clear_buffer()


def test_first_test_dirties_everything(semblance):
    SPARK.sql.return_value = 42  # the shared bootstrap mocks: reset, not replaced, between tests
    DBUTILS.credentials.getServiceCredentialsProvider.return_value = "leak"
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


def test_second_test_sees_a_clean_slate(semblance):
    assert SPARK.sql.return_value != 42
    assert DBUTILS.credentials.getServiceCredentialsProvider.return_value != "leak"
    assert TABLE_LOG_HANDLER._table is not None  # patched for this test only
    assert "dags" not in semblance.tables.tables  # fresh store: no rows from the previous test


def test_handler_table_is_unresolved_outside_the_fixture():
    assert TABLE_LOG_HANDLER._table is None
