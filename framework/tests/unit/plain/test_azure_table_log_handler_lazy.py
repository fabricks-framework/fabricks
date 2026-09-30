from unittest.mock import MagicMock

from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler


def test_factory_is_not_called_until_the_table_is_used():
    factory = MagicMock(return_value=MagicMock(name="table"))
    handler = AzureTableLogHandler(table=factory)

    factory.assert_not_called()
    assert handler.table is factory.return_value
    assert handler.table is factory.return_value
    factory.assert_called_once_with()


def test_a_table_instance_is_used_as_is():
    table = AzureTable("t", connection_string="UseDevelopmentStorage=true")  # constructing does no I/O
    handler = AzureTableLogHandler(table=table)

    assert handler.table is table


def test_flush_with_an_empty_buffer_does_not_resolve_the_table():
    factory = MagicMock()
    handler = AzureTableLogHandler(table=factory)

    handler.flush()

    factory.assert_not_called()


def test_flush_upserts_a_non_empty_buffer_and_clears_it():
    seen: list[dict] = []
    table = MagicMock(spec=AzureTable)
    table.upsert.side_effect = lambda rows: seen.extend(rows)  # the buffer is cleared right after upsert
    handler = AzureTableLogHandler(table=table)
    handler.buffer.append({"PartitionKey": "p", "RowKey": "1"})

    handler.flush()

    assert seen == [{"PartitionKey": "p", "RowKey": "1"}]
    assert handler.buffer == []
