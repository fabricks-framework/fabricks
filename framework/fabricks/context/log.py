import logging
from typing import Final

from fabricks.context.runtime import IS_DEBUGMODE
from fabricks.utils.log import get_logger

logger, _ = get_logger("logs", logging.DEBUG, table=None, debugmode=IS_DEBUGMODE)

DEFAULT_LOGGER: Final[logging.Logger] = logger
