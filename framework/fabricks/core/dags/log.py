import logging
from typing import Final, cast, TYPE_CHECKING

from fabricks.context.runtime import FABRICKS_STORAGE, IS_UNITY_CATALOG, SECRET_SCOPE, FABRICKS_STORAGE_CREDENTIAL
from fabricks.context.spark_session import DBUTILS
from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler, get_logger
if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

storage_account = FABRICKS_STORAGE.get_storage_account()

def _get_table():
    if not IS_UNITY_CATALOG:
        from fabricks.context.secret import AccessKey, get_secret_from_secret_scope

        secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
        access_key = cast(AccessKey, secret).key
        credential : "TokenCredential | None" = None
    else:
        import os
        
        access_key = os.environ.get("FABRICKS_ACCESS_KEY")
        credential : "TokenCredential | None" = DBUTILS.credentials.getServiceCredentialsProvider(FABRICKS_STORAGE_CREDENTIAL) if FABRICKS_STORAGE_CREDENTIAL else None # type: ignore
        if not access_key:
            from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
            secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
            access_key = cast(AccessKey, secret).key
        assert credential or access_key, "path_options.storage_credential or FABRICKS_ACCESS_KEY must be set"
    return AzureTable("dags", storage_account=storage_account, access_key=access_key, credential=credential)


table =  _get_table()
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
