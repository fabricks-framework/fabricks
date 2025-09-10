from typing import Optional, cast

from fabricks.context import DBUTILS, FABRICKS_STORAGE_CREDENTIAL, IS_UNITY_CATALOG, SECRET_SCOPE


def _get_access_key_from_secret_scope(storage_account: str) -> str:
    from fabricks.context.secret import AccessKey, get_secret_from_secret_scope

    secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
    return cast(AccessKey, secret).key


def _get_access_key_from_os() -> Optional[str]:
    import os

    return os.environ.get("FABRICKS_ACCESS_KEY")


def get_connection_info(storage_account: str) -> dict:
    credential = None

    if not IS_UNITY_CATALOG:
        access_key = _get_access_key_from_secret_scope(storage_account)

    else:
        access_key = _get_access_key_from_os()
        if not access_key:
            access_key = _get_access_key_from_secret_scope(storage_account)

        if FABRICKS_STORAGE_CREDENTIAL:
            assert DBUTILS
            credential = DBUTILS.credentials.getServiceCredentialsProvider(FABRICKS_STORAGE_CREDENTIAL)  # type: ignore

        assert credential or access_key

    return {
        "storage_account": storage_account,
        "access_key": access_key,
        "credential": credential,
    }
