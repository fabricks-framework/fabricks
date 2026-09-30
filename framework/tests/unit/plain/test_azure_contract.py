"""The same scenarios through the real AzureTable/AzureQueue wrappers against the semblance fakes and,
opt-in, a real Azurite emulator, so the fakes cannot drift from Azure unnoticed.

Run Azurite yourself (no Docker):   npx azurite --silent --location "$(mktemp -d)"
then:  FABRICKS_TEST_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true" \
       just test-plain tests/unit/plain/test_azure_contract.py
"""

import os
import uuid

from azure.core.exceptions import HttpResponseError
import pytest

from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable

_AZURITE = os.environ.get("FABRICKS_TEST_AZURITE_CONNECTION_STRING")


@pytest.fixture(
    params=[
        "fake",
        pytest.param(
            "azurite", marks=pytest.mark.skipif(not _AZURITE, reason="set FABRICKS_TEST_AZURITE_CONNECTION_STRING")
        ),
    ]
)
def backend(request):
    if request.param == "fake":
        request.getfixturevalue("semblance")
        return "UseDevelopmentStorage=true"
    return _AZURITE


@pytest.fixture
def table(backend):
    name = f"t{uuid.uuid4().hex[:12]}"
    with AzureTable(name, connection_string=backend) as t:
        t.create_if_not_exists()
        yield t
        t.drop()


@pytest.fixture
def queue(backend):
    name = f"q{uuid.uuid4().hex[:12]}"
    with AzureQueue(name, connection_string=backend) as q:
        q.create_if_not_exists()
        yield q
        q.delete()


def test_upsert_query_round_trip_and_filters(table):
    table.upsert(
        [
            {"PartitionKey": "b", "RowKey": "2", "Status": "pending", "JobId": "j2"},
            {"PartitionKey": "a", "RowKey": "1", "Status": "pending", "JobId": "j1"},
            {"PartitionKey": "a", "RowKey": "2", "Status": "ok", "JobId": "j1"},
        ]
    )

    assert [(r["PartitionKey"], r["RowKey"]) for r in table.query("")] == [("a", "1"), ("a", "2"), ("b", "2")]
    assert [r["RowKey"] for r in table.query("PartitionKey eq 'a' and JobId eq 'j1' and Status eq 'ok'")] == ["2"]


def test_upsert_merges_properties(table):
    table.upsert({"PartitionKey": "p", "RowKey": "1", "A": "1", "B": "1"})
    table.upsert({"PartitionKey": "p", "RowKey": "1", "B": "2"})

    row = table.query("PartitionKey eq 'p'")[0]
    assert (row["A"], row["B"]) == ("1", "2")


def test_deleting_a_missing_row_raises_an_azure_http_error(table):
    with pytest.raises(HttpResponseError):
        table.delete({"PartitionKey": "p", "RowKey": "never-existed"})


def test_creating_an_existing_queue_is_idempotent_and_keeps_its_messages(queue):
    queue.send("kept")

    queue.create_if_not_exists()  # the raw client call raises ResourceExistsError; the wrapper suppresses it

    assert queue.receive() == "kept"


def test_queue_delivers_each_message_exactly_once(queue):
    for m in ("one", "two", "three"):
        queue.send(m)

    received = [queue.receive(), queue.receive(), queue.receive()]

    assert sorted(received) == ["one", "three", "two"]
    assert queue.receive() is None
