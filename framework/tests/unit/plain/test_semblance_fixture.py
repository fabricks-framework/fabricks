import sys

from azure.core.exceptions import ResourceNotFoundError
import pytest

from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable
from tests.semblance.fixture import Semblance


def test_azure_wrappers_run_against_the_fakes(semblance):
    with AzureTable("t1", connection_string="UseDevelopmentStorage=true") as table:
        table.upsert([{"PartitionKey": "p", "RowKey": "1", "S": "a"}])
        assert table.query("PartitionKey eq 'p'")[0]["S"] == "a"
    assert semblance.table("t1").rows(PartitionKey="p")[0]["RowKey"] == "1"

    with AzureQueue("q1", connection_string="UseDevelopmentStorage=true") as queue:
        queue.create_if_not_exists()
        queue.create_if_not_exists()  # a second create is a no-op
        queue.send({"a": 1})
        assert queue.receive() == '{"a": 1}'
    assert semblance.queue("q1").sent == ['{"a": 1}']
    assert semblance.queue("q1").pending == []


def test_runtime_module_carries_the_fake_dbutils(semblance):
    from databricks.sdk.runtime import dbutils

    assert dbutils is semblance.dbutils
    semblance.widgets["schedule"] = "daily"
    assert sys.modules["databricks.sdk.runtime"].dbutils.widgets.get("schedule") == "daily"


def test_time_sleep_is_neutralised(semblance):
    import time

    start = time.monotonic()
    time.sleep(30)
    assert time.monotonic() - start < 1


def test_each_semblance_starts_with_empty_state(semblance, tmp_path):
    semblance.widgets["leak"] = "1"
    semblance.task_values["leak"] = "1"
    semblance.table("t").seed([{"PartitionKey": "p", "RowKey": "1"}])
    semblance.queue("q").create()
    semblance.queue("q").send("m")

    fresh = Semblance(tmp_path)

    assert fresh.widgets == {}
    assert fresh.task_values == {}
    assert fresh.notebook_calls == []
    with pytest.raises(ResourceNotFoundError):
        fresh.table("t").rows()
    with pytest.raises(ResourceNotFoundError):
        _ = fresh.queue("q").sent
