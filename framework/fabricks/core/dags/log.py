import logging
from typing import Final

from fabricks.core.dags.utils import get_table
from fabricks.utils.log import AzureTableLogHandler, get_logger

table = get_table()
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
