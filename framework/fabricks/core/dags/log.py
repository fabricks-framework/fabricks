import logging
from typing import Final

from fabricks.core.dags.utils import get_table
from fabricks.utils.log import AzureTableLogHandler, get_logger

# get_table is passed as a factory: resolving the storage account at import would need a real
# Azure FileSharePath, which local runs (LocalFileSharePath) do not have
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=get_table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
