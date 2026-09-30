from azure.core.exceptions import ResourceNotFoundError
import pytest

from tests.semblance.azure_fakes import (
    QueueClientFactory,
    QueueStore,
    QueueView,
    TableServiceFactory,
    TableStore,
    TableView,
    parse_filter,
)


@pytest.fixture
def tables():
    return TableStore()


@pytest.fixture
def table_client(tables):
    return TableServiceFactory(tables).from_connection_string("x").create_table_if_not_exists(table_name="t")


def test_parse_filter_grammar():
    assert parse_filter("") == []
    assert parse_filter("PartitionKey eq 'a' and JobId eq 'b'") == [("PartitionKey", "a"), ("JobId", "b")]
    assert parse_filter("Name eq 'it''s'") == [("Name", "it's")]


@pytest.mark.parametrize("bad", ["Rank gt 1", "A eq 1", "A eq 'x' or B eq 'y'", "not A eq 'x'"])
def test_parse_filter_rejects_anything_else(bad):
    with pytest.raises(NotImplementedError):
        parse_filter(bad)


def test_upsert_query_round_trip_sorted_by_partition_and_row_key(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "b", "RowKey": "2", "V": "x"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "a", "RowKey": "9", "V": "y"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "a", "RowKey": "1", "V": "z"})])

    rows = list(table_client.query_entities(""))
    assert [(r["PartitionKey"], r["RowKey"]) for r in rows] == [("a", "1"), ("a", "9"), ("b", "2")]
    assert [r["V"] for r in table_client.query_entities("PartitionKey eq 'a' and V eq 'y'")] == ["y"]


def test_upsert_merges_and_returned_rows_are_copies(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1", "A": "1", "B": "1"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1", "B": "2"})])
    row = next(iter(table_client.query_entities("")))
    assert row["A"] == "1"
    assert row["B"] == "2"

    row["A"] = "mutated"
    assert next(iter(table_client.query_entities("")))["A"] == "1"


def test_delete_of_a_missing_row_raises_and_the_transaction_is_atomic(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1"})])
    with pytest.raises(ResourceNotFoundError):
        table_client.submit_transaction(
            [("delete", {"PartitionKey": "p", "RowKey": "1"}), ("delete", {"PartitionKey": "p", "RowKey": "gone"})]
        )
    assert len(list(table_client.query_entities(""))) == 1


def test_a_transaction_must_share_one_partition(table_client):
    with pytest.raises(ValueError, match="share a PartitionKey"):
        table_client.submit_transaction(
            [("upsert", {"PartitionKey": "a", "RowKey": "1"}), ("upsert", {"PartitionKey": "b", "RowKey": "1"})]
        )


def test_unsupported_query_arguments_and_operations_fail_loudly(table_client):
    with pytest.raises(NotImplementedError):
        table_client.query_entities("", select=["A"])
    with pytest.raises(NotImplementedError):
        table_client.submit_transaction([("update", {"PartitionKey": "a", "RowKey": "1"})])


def test_state_is_shared_across_client_instances_and_drop_removes_the_table(tables):
    factory = TableServiceFactory(tables)
    factory(endpoint="e", credential=None).create_table_if_not_exists(table_name="t").submit_transaction(
        [("upsert", {"PartitionKey": "p", "RowKey": "1"})]
    )
    second = factory.from_connection_string("x").create_table_if_not_exists(table_name="t")
    assert len(list(second.query_entities(""))) == 1

    factory.from_connection_string("x").delete_table("t")
    with pytest.raises(ResourceNotFoundError):
        list(second.query_entities(""))


def test_table_view_seed_and_rows(tables):
    view = TableView(tables, "t")
    view.seed([{"PartitionKey": "p", "RowKey": "1", "S": "a"}, {"PartitionKey": "p", "RowKey": "2", "S": "b"}])
    assert [r["RowKey"] for r in view.rows(PartitionKey="p", S="b")] == ["2"]
    with pytest.raises(ResourceNotFoundError):
        TableView(tables, "typo").rows()
    with pytest.raises(TypeError):
        view.rows(Rank=1)


@pytest.fixture
def queues():
    return QueueStore()


def test_queue_requires_creation_and_creating_an_existing_queue_is_a_noop(queues):
    client = QueueClientFactory(queues).from_connection_string("x", queue_name="q")
    with pytest.raises(ResourceNotFoundError):
        client.send_message("a")
    client.create_queue()
    client.send_message("a")
    client.create_queue()  # Azure returns 204 for an existing queue with identical metadata
    assert QueueView(queues, "q").pending == ["a"]


def test_queue_is_fifo_and_receive_removes_but_sent_keeps_history(queues):
    client = QueueClientFactory(queues)(account_url="u", queue_name="q", credential=None)
    client.create_queue()
    assert client.receive_message() is None
    client.send_message("one")
    client.send_message("two")

    msg = client.receive_message()
    assert msg.content == "one"
    client.delete_message(msg)

    view = QueueView(queues, "q")
    assert view.pending == ["two"]
    assert view.sent == ["one", "two"]

    client.clear_messages()
    assert view.pending == []
    assert view.sent == ["one", "two"]
    client.delete_queue()
    with pytest.raises(ResourceNotFoundError):
        _ = view.sent


def test_queue_only_supports_string_content(queues):
    client = QueueClientFactory(queues).from_connection_string("x", queue_name="q")
    client.create_queue()
    with pytest.raises(TypeError):
        client.send_message({"a": 1})
