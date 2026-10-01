"""AzureTableLogHandler resolves its table lazily: building or flushing an empty handler does no I/O."""

from pyspark.sql import DataFrame
import pytest

from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler

ROW = {"PartitionKey": "p", "RowKey": "1"}


def test_factory_is_not_called_until_the_table_is_used(semblance):
    calls: list[int] = []

    def factory() -> AzureTable:
        calls.append(1)
        return AzureTable("t", connection_string="UseDevelopmentStorage=true")

    handler = AzureTableLogHandler(table=factory)
    assert calls == []

    first = handler.table
    second = handler.table

    assert calls == [1], "the factory must run once, on first use"
    assert first is second


def test_a_table_instance_is_used_as_is():
    table = AzureTable("t", connection_string="UseDevelopmentStorage=true")  # constructing does no I/O
    handler = AzureTableLogHandler(table=table)

    assert handler.table is table


def test_flush_with_an_empty_buffer_does_not_resolve_the_table():
    def factory() -> AzureTable:
        raise AssertionError("flushing an empty buffer must not resolve the table")

    AzureTableLogHandler(table=factory).flush()


def test_flush_upserts_a_non_empty_buffer_and_clears_it(semblance):
    table = AzureTable("t", connection_string="UseDevelopmentStorage=true")
    handler = AzureTableLogHandler(table=table)
    handler.buffer.append(dict(ROW))

    handler.flush()

    assert semblance.table("t").rows(PartitionKey="p") == [ROW]
    assert handler.buffer == []


def test_flush_keeps_the_buffer_when_the_upsert_fails():
    class _FailingTable(AzureTable):
        def upsert(self, data: list | DataFrame | dict) -> None:
            raise ConnectionError("storage unreachable")

    handler = AzureTableLogHandler(table=_FailingTable("t", connection_string="UseDevelopmentStorage=true"))
    handler.buffer.append(dict(ROW))

    with pytest.raises(ConnectionError, match="storage unreachable"):
        handler.flush()

    assert handler.buffer == [ROW], "rows not yet persisted must survive a failed flush"
    handler.buffer.clear()  # logging.shutdown flushes every handler at exit; don't re-raise there
