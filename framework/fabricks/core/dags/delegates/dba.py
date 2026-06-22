from __future__ import annotations

import os
from typing import Any, Optional, cast

from azure.core.exceptions import AzureError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from fabricks.context import DBUTILS, FABRICKS_STORAGE, FABRICKS_STORAGE_CREDENTIAL, IS_UNITY_CATALOG, SECRET_SCOPE
from fabricks.core.dags.protocols import BaseDagsProtocol
from fabricks.utils.azure_table import AzureTable


def _get_access_key_from_secret_scope(storage_account: str) -> str:
    from fabricks.context.secret import AccessKey, get_secret_from_secret_scope

    secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
    return cast(AccessKey, secret).key


def _get_access_key_from_os() -> Optional[str]:
    return os.environ.get("FABRICKS_ACCESS_KEY")


def get_connection_info(storage_account: str) -> dict[str, Any]:
    credential = None

    if not IS_UNITY_CATALOG:
        access_key = _get_access_key_from_secret_scope(storage_account)

    else:
        access_key = _get_access_key_from_os()
        if not access_key:
            access_key = _get_access_key_from_secret_scope(storage_account)

        if FABRICKS_STORAGE_CREDENTIAL:
            assert DBUTILS
            credential = DBUTILS.credentials.getServiceCredentialsProvider(FABRICKS_STORAGE_CREDENTIAL)  # pyright: ignore[reportCallIssue]

        assert credential or access_key

    return {
        "storage_account": storage_account,
        "access_key": access_key,
        "credential": credential,
    }


def get_log_table() -> AzureTable:
    storage_account = FABRICKS_STORAGE.get_storage_account()
    cx = get_connection_info(storage_account)
    return AzureTable(
        "dags",
        storage_account=storage_account,
        access_key=cx["access_key"],
        credential=cx["credential"],
    )


class DagDba:
    def __init__(self, dags: BaseDagsProtocol):
        self._dags = dags
        self._connection_info: Optional[dict[str, Any]] = None
        self._table: Optional[AzureTable] = None

    @property
    def storage_account(self) -> str:
        return FABRICKS_STORAGE.get_storage_account()

    def get_connection_info(self) -> dict[str, Any]:
        if not self._connection_info:
            self._connection_info = get_connection_info(self.storage_account)
        return self._connection_info

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((Exception, AzureError)),
        reraise=True,
    )
    def get_table(self) -> AzureTable:
        if not self._table:
            cs = self.get_connection_info()
            self._table = AzureTable(f"t{self._dags.schedule_id}", **dict(cs))
        if self._table is None:
            raise ValueError("Azure table for logs not found")
        return self._table

    def __enter__(self):
        return self._dags

    def __exit__(self, *args, **kwargs):
        if self._table is not None:
            self._table.__exit__()
