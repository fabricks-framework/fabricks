import contextlib
import json
from typing import TYPE_CHECKING, Self

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


class AzureQueue:
    def __init__(
        self,
        name: str,
        storage_account: str | None = None,
        access_key: str | None = None,
        connection_string: str | None = None,
        credential: "TokenCredential | None" = None,
    ) -> None:
        self.name = name
        self.storage_account = storage_account
        if connection_string is None:
            assert storage_account
            assert access_key or credential, "Either access_key or credential must be provided"
            self.storage_account = storage_account
            self.access_key = access_key
            self.credential = credential
            connection_string = (
                f"DefaultEndpointsProtocol=https;AccountName={self.storage_account};AccountKey={self.access_key};EndpointSuffix=core.windows.net"
                if access_key
                else None
            )

        assert connection_string
        self.connection_string = connection_string
        self._queue_client = None

    @property
    def queue_client(self) -> QueueClient:
        if not self._queue_client:
            if self.connection_string is not None:
                self._queue_client = QueueClient.from_connection_string(self.connection_string, queue_name=self.name)
            else:
                assert self.storage_account, "storage_account must be provided"
                assert self.access_key or self.credential, "Either access_key or credential must be provided"
                self._queue_client = QueueClient(
                    account_url=f"https://{self.storage_account}.queue.core.windows.net",
                    queue_name=self.name,
                    credential=self.access_key if self.access_key else self.credential,
                )
        return self._queue_client

    def create_if_not_exists(self) -> None:
        with contextlib.suppress(ResourceExistsError):
            self.queue_client.create_queue()

    @property
    def sentinel(self) -> str:
        return "SENTINEL"

    def clear(self) -> None:
        self.queue_client.clear_messages()

    def send(self, message: str | dict) -> None:
        if isinstance(message, dict):
            message = json.dumps(message)
        self.queue_client.send_message(message)

    def send_sentinel(self) -> None:
        self.send(self.sentinel)

    def receive(self) -> str | None:
        msg = self.queue_client.receive_message()
        if msg:
            self.queue_client.delete_message(msg)
            return msg.content
        return None

    def delete(self) -> None:
        self.queue_client.delete_queue()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: object) -> None:
        if self._queue_client is not None:
            self._queue_client.close()
