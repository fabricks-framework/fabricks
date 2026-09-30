"""In-memory stand-ins for the Azure Table and Queue SDK clients Fabricks uses.

Models only what fabricks/utils/azure_table.py and azure_queue.py call. State lives in a store owned
by the caller, so it is fresh per test and shared by every client instance (AzureTable builds a new
TableServiceClient on each access when it has no connection string).
"""

from collections import deque
import contextlib
import copy
import re
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.queue import QueueMessage

_CLAUSE = re.compile(r"^(\w+) eq '((?:[^']|'')*)'$")


def parse_filter(query: str) -> list[tuple[str, str]]:
    """The only grammar Fabricks generates: `` or `Field eq 'v' and Field2 eq 'w'`.

    # ceiling: splits on " and ", so a value containing " and " is not supported.
    """
    query = query.strip()
    if not query:
        return []
    clauses = []
    for part in query.split(" and "):
        match = _CLAUSE.match(part.strip())
        if match is None:
            raise NotImplementedError(
                f"semblance supports only `Field eq 'value'` clauses joined by ' and ', got: {query!r}"
            )
        clauses.append((match.group(1), match.group(2).replace("''", "'")))
    return clauses


# ---- tables ----------------------------------------------------------------


class TableStore:
    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}


class FakeTableClient:
    def __init__(self, store: TableStore, name: str) -> None:
        self._store = store
        self.table_name = name

    def _rows(self) -> dict[tuple[str, str], dict[str, Any]]:
        try:
            return self._store.tables[self.table_name]
        except KeyError:
            raise ResourceNotFoundError(f"table {self.table_name!r} does not exist") from None

    def query_entities(self, query_filter: str = "", **kwargs: Any):
        if kwargs:
            raise NotImplementedError(f"unsupported query_entities arguments: {sorted(kwargs)}")
        clauses = parse_filter(query_filter)
        rows = self._rows()
        # sorted by (PartitionKey, RowKey) like the real service, not insertion order
        return iter(
            [copy.deepcopy(row) for _key, row in sorted(rows.items()) if all(row.get(f) == v for f, v in clauses)]
        )

    def submit_transaction(self, operations, **kwargs: Any):
        if kwargs:
            raise NotImplementedError(f"unsupported submit_transaction arguments: {sorted(kwargs)}")
        rows = self._rows()
        ops = list(operations)
        if len({entity["PartitionKey"] for _op, entity, *_ in ops}) > 1:
            raise ValueError("all entities in a transaction must share a PartitionKey")

        staged = {key: dict(row) for key, row in rows.items()}  # atomic: apply to a copy, commit at the end
        for op, entity, *_ in ops:
            key = (entity["PartitionKey"], entity["RowKey"])
            if op == "upsert":
                staged.setdefault(key, {}).update(copy.deepcopy(dict(entity)))  # merge, the SDK's default mode
            elif op == "delete":
                if key not in staged:
                    raise ResourceNotFoundError(f"entity {key} does not exist in table {self.table_name!r}")
                del staged[key]
            else:
                raise NotImplementedError(f"unsupported transaction operation {op!r}")
        rows.clear()
        rows.update(staged)
        return []


class FakeTableServiceClient:
    def __init__(self, store: TableStore) -> None:
        self._store = store

    def create_table_if_not_exists(self, table_name: str, **kwargs: Any) -> FakeTableClient:
        self._store.tables.setdefault(table_name, {})
        return FakeTableClient(self._store, table_name)

    def delete_table(self, table_name: str, **kwargs: Any) -> None:
        if table_name not in self._store.tables:
            raise ResourceNotFoundError(f"table {table_name!r} does not exist")
        del self._store.tables[table_name]

    def close(self) -> None:
        pass


class TableServiceFactory:
    """Stands in for the `TableServiceClient` class: callable plus `from_connection_string`."""

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> FakeTableServiceClient:
        return FakeTableServiceClient(self.store)

    def from_connection_string(self, conn_str: str, **kwargs: Any) -> FakeTableServiceClient:
        return FakeTableServiceClient(self.store)


class TableView:
    """Test-side handle on one table."""

    def __init__(self, store: TableStore, name: str) -> None:
        self._store = store
        self._name = name

    def seed(self, rows: list[dict]) -> None:
        client = FakeTableServiceClient(self._store).create_table_if_not_exists(self._name)
        for row in rows:
            client.submit_transaction([("upsert", row)])

    def rows(self, **where: str) -> list[dict]:
        if self._name not in self._store.tables:
            raise ResourceNotFoundError(f"table {self._name!r} does not exist; tables: {sorted(self._store.tables)}")
        for field, value in where.items():
            if not isinstance(value, str):
                raise TypeError(f"rows() filters by string equality only; {field}={value!r} is not a str")
        query = " and ".join(f"{f} eq '{v.replace(chr(39), chr(39) * 2)}'" for f, v in where.items())
        return list(FakeTableClient(self._store, self._name).query_entities(query))


# ---- queues ----------------------------------------------------------------


class _FakeQueue:
    def __init__(self) -> None:
        self.pending: deque[str] = deque()
        self.sent: list[str] = []


class QueueStore:
    def __init__(self) -> None:
        self.queues: dict[str, _FakeQueue] = {}


class FakeQueueClient:
    # no visibility timeout / dequeue count: AzureQueue only ever receives then deletes
    def __init__(self, store: QueueStore, queue_name: str) -> None:
        self._store = store
        self.queue_name = queue_name

    def _queue(self) -> _FakeQueue:
        try:
            return self._store.queues[self.queue_name]
        except KeyError:
            raise ResourceNotFoundError(f"queue {self.queue_name!r} does not exist") from None

    def create_queue(self, **kwargs: Any) -> None:
        # Azurite (contract-tested) answers 409 QueueAlreadyExists; AzureQueue.create_if_not_exists suppresses it
        if self.queue_name in self._store.queues:
            raise ResourceExistsError(f"queue {self.queue_name!r} already exists")
        self._store.queues[self.queue_name] = _FakeQueue()

    def send_message(self, content: str, **kwargs: Any) -> QueueMessage:
        if not isinstance(content, str):
            raise TypeError("semblance queue fake supports str content only")
        queue = self._queue()
        queue.pending.append(content)
        queue.sent.append(content)
        return QueueMessage(content=content)

    def receive_message(self, **kwargs: Any) -> QueueMessage | None:
        queue = self._queue()
        return QueueMessage(content=queue.pending.popleft()) if queue.pending else None

    def delete_message(self, message: Any, pop_receipt: str | None = None, **kwargs: Any) -> None:
        self._queue()  # the message was already removed on receive

    def clear_messages(self, **kwargs: Any) -> None:
        self._queue().pending.clear()

    def delete_queue(self, **kwargs: Any) -> None:
        self._queue()
        del self._store.queues[self.queue_name]

    def close(self) -> None:
        pass


class QueueClientFactory:
    """Stands in for the `QueueClient` class: callable plus `from_connection_string`."""

    def __init__(self, store: QueueStore) -> None:
        self.store = store

    def __call__(
        self, account_url: str | None = None, queue_name: str | None = None, credential: Any = None, **kwargs: Any
    ) -> FakeQueueClient:
        assert queue_name, "queue_name is required"
        return FakeQueueClient(self.store, queue_name)

    def from_connection_string(self, conn_str: str, queue_name: str, **kwargs: Any) -> FakeQueueClient:
        return FakeQueueClient(self.store, queue_name)


class QueueView:
    """Test-side handle on one queue."""

    def __init__(self, store: QueueStore, name: str) -> None:
        self._store = store
        self._name = name

    def _queue(self) -> _FakeQueue:
        if self._name not in self._store.queues:
            raise ResourceNotFoundError(f"queue {self._name!r} does not exist; queues: {sorted(self._store.queues)}")
        return self._store.queues[self._name]

    def create(self) -> None:
        with contextlib.suppress(ResourceExistsError):
            FakeQueueClient(self._store, self._name).create_queue()

    def send(self, content: str) -> None:
        FakeQueueClient(self._store, self._name).send_message(content)

    @property
    def sent(self) -> list[str]:
        """Every message ever sent, in order (receive deletes from `pending`, not from here)."""
        return list(self._queue().sent)

    @property
    def pending(self) -> list[str]:
        return list(self._queue().pending)
