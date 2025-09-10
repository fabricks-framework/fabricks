import logging
from typing import Final

from fabricks.context.runtime import FABRICKS_STORAGE
from fabricks.core.dags.utils import get_connection_info
from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler, get_logger


def _get_table():
    storage_account = FABRICKS_STORAGE.get_storage_account()

    cx = get_connection_info(storage_account)

    return AzureTable(
        "dags", storage_account=storage_account, access_key=cx["access_key"], credential=cx["credential"]
    )


table = _get_table()
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
