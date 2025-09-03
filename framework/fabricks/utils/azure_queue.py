import json
from typing import TYPE_CHECKING, Optional, Union

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueClient

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


class AzureQueue:
    def __init__(
        self,
        name: str,
        storage_account: Optional[str] = None,
        access_key: Optional[str] = None,
        connection_string: Optional[str] = None,
        credential: "Optional[TokenCredential]" = None,
    ):
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
                assert self.storage_account and (self.access_key or self.credential), (
                    "Either access_key or credential must be provided"
                )
                self._queue_client = QueueClient(
                    account_url=f"https://{self.storage_account}.queue.core.windows.net",
                    queue_name=self.name,
                    credential=self.access_key if self.access_key else self.credential,
                )
        return self._queue_client

    def create_if_not_exists(self):
        try:
            self.queue_client.create_queue()
        except ResourceExistsError:
            pass

    @property
    def sentinel(self):
        return "SENTINEL"

    def clear(self):
        self.queue_client.clear_messages()

    def send(self, message: Union[str, dict]):
        if isinstance(message, dict):
            message = json.dumps(message)
        # print("sending ->", message)
        self.queue_client.send_message(message)

    def send_sentinel(self):
        # print("sentinel", self.sentinel)
        self.send(self.sentinel)

    def receive(self):
        msg = self.queue_client.receive_message()
        if msg:
            self.queue_client.delete_message(msg)
            # print("receiving ->", msg.content)
            return msg.content
        return None

    def delete(self):
        self.queue_client.delete_queue()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self._queue_client is not None:
            self._queue_client.close()
