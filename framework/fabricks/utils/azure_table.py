import time
from typing import List, Optional, Union

from azure.data.tables import TableClient, TableServiceClient
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class AzureTable:
    def __init__(
        self,
        name: str,
        storage_account: Optional[str] = None,
        access_key: Optional[str] = None,
        connection_string: Optional[str] = None,
    ):
        self.name = name

        if connection_string is None:
            assert storage_account
            assert access_key
            self.storage_account = storage_account
            self.access_key = access_key

            connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account};AccountKey={self.access_key};EndpointSuffix=core.windows.net"

        assert connection_string
        self.connection_string = connection_string

        self._table_client = None

    @property
    def table_service_client(self) -> TableServiceClient:
        if not self._table_client:
            self._table_client = TableServiceClient.from_connection_string(self.connection_string)
        return self._table_client

    @property
    def table(self) -> TableClient:
        return self.create_if_not_exists()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception)),
        reraise=True,
    )
    def create_if_not_exists(self) -> TableClient:
        return self.table_service_client.create_table_if_not_exists(table_name=self.name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception)),
        reraise=True,
    )
    def drop(self):
        self.table_service_client.delete_table(self.name)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception)),
        reraise=True,
    )
    def query(self, query: str) -> List:
        return list(self.table.query_entities(query))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception)),
        reraise=True,
    )
    def list_all(self) -> List:
        return self.query("")

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs) -> None:
        if self._table_client is not None:
            self._table_client.close()

    def submit(self, operations: List, retry: Optional[bool] = True):
        try:
            partitions = set()
            for d in operations:
                partitions.add(d[1]["PartitionKey"])

            for p in partitions:
                _operations = [d for d in operations if d[1].get("PartitionKey") == p]
                t = 50
                if len(_operations) < t:
                    self.table.submit_transaction(_operations)
                else:
                    transactions = [_operations[i : i + t] for i in range(0, len(_operations), t)]
                    for transaction in transactions:
                        self.table.submit_transaction(transaction)
        except Exception as e:
            if retry:
                time.sleep(10)
                self.submit(operations, retry=False)
            else:
                raise e

    def delete(self, data: Union[List, DataFrame, dict]):
        if isinstance(data, (DataFrame, CDataFrame)):
            data = [row.asDict() for row in data.collect()]
        elif not isinstance(data, List):
            data = [data]

        operations = [("delete", d) for d in data]
        self.submit(operations)

    def upsert(self, data: Union[List, DataFrame, dict]):
        if isinstance(data, (DataFrame, CDataFrame)):
            data = [row.asDict() for row in data.collect()]
        elif not isinstance(data, List):
            data = [data]

        operations = [("upsert", d) for d in data]
        self.submit(operations)

    def truncate_partition(self, partition: str):
        data = self.query(f"PartitionKey eq '{partition}'")
        self.delete(data)

    def truncate_all_partitions(self):
        for p in self.list_all_partitions():
            self.truncate_partition(p)

    def list_all_partitions(self) -> List:
        partitions = set()
        for d in self.list_all():
            partitions.add(d["PartitionKey"])
        return sorted(list(partitions))
