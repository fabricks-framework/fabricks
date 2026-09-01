from typing import TYPE_CHECKING, Any, Self

from azure.data.tables import TableClient, TableServiceClient
from pyspark.sql import DataFrame
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.utils._types import DataFrameLike

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


class AzureTable:
    def __init__(
        self,
        name: str,
        storage_account: str | None = None,
        access_key: str | None = None,
        connection_string: str | None = None,
        credential: "TokenCredential | None" = None,
    ) -> None:
        self.name = name

        if connection_string is None:
            assert storage_account, "storage_account must be provided if connection_string is not set"
            assert access_key or credential, "Either access_key or credential must be provided"
            self.storage_account = storage_account
            self.access_key = access_key
            self.credential = credential
            self.storage_account = storage_account

            connection_string = (
                f"DefaultEndpointsProtocol=https;AccountName={self.storage_account};AccountKey={self.access_key};EndpointSuffix=core.windows.net"
                if access_key
                else None
            )

        assert connection_string
        self.connection_string = connection_string

        self._table_client = None

    @property
    def table_service_client(self) -> TableServiceClient:
        if not self._table_client:
            if self.connection_string is None:
                return TableServiceClient(
                    endpoint=f"https://{self.storage_account}.table.core.windows.net", credential=self.credential
                )
            self._table_client = TableServiceClient.from_connection_string(self.connection_string)
        return self._table_client

    @property
    def table(self) -> TableClient:
        return self.create_if_not_exists()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def create_if_not_exists(self) -> TableClient:
        return self.table_service_client.create_table_if_not_exists(table_name=self.name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def drop(self) -> None:
        self.table_service_client.delete_table(self.name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def query(self, query: str) -> list:
        return list(self.table.query_entities(query))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def list_all(self) -> list:
        return self.query("")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: object) -> None:
        if self._table_client is not None:
            self._table_client.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _submit_with_retry(self, data: list[tuple[str, Any]]) -> None:
        self.table.submit_transaction(data)

    def submit(self, operations: list) -> None:
        partitions = set()
        for d in operations:
            partitions.add(d[1]["PartitionKey"])

        for p in partitions:
            _operations = [d for d in operations if d[1].get("PartitionKey") == p]
            t = 50
            if len(_operations) < t:
                self._submit_with_retry(_operations)
            else:
                transactions = [_operations[i : i + t] for i in range(0, len(_operations), t)]
                for transaction in transactions:
                    self._submit_with_retry(transaction)

    def delete(self, data: list | DataFrame | dict) -> None:
        if isinstance(data, DataFrameLike):
            data = data.toPandas().to_dict("records")
        elif not isinstance(data, list):
            data = [data]

        operations = [("delete", d) for d in data]
        self.submit(operations)

    def upsert(self, data: list | DataFrame | dict) -> None:
        if isinstance(data, DataFrameLike):
            data = data.toPandas().to_dict("records")
        elif not isinstance(data, list):
            data = [data]

        operations = [("upsert", d) for d in data]
        self.submit(operations)

    def truncate_partition(self, partition: str) -> None:
        data = self.query(f"PartitionKey eq '{partition}'")
        self.delete(data)

    def truncate_all_partitions(self) -> None:
        for p in self.list_all_partitions():
            self.truncate_partition(p)

    def list_all_partitions(self) -> list:
        partitions = set()
        for d in self.list_all():
            partitions.add(d["PartitionKey"])
        return sorted(partitions)
