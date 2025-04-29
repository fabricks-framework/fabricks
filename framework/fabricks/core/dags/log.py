import logging
from typing import Final, cast

from fabricks.context.runtime import FABRICKS_STORAGE, IS_UNITY_CATALOG, SECRET_SCOPE
from fabricks.context.secret import AccessKey, get_secret_from_secret_scope
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler, get_logger

storage_account = FABRICKS_STORAGE.get_storage_account()

if not IS_UNITY_CATALOG:
    from fabricks.context.secret import AccessKey, get_secret_from_secret_scope

    secret = get_secret_from_secret_scope(SECRET_SCOPE, f"{storage_account}-access-key")
    access_key = cast(AccessKey, secret).key
else:
    import os

    access_key = os.environ["FABRICKS_ACCESS_KEY"]


table = AzureTable("dags", storage_account=storage_account, access_key=access_key)
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
