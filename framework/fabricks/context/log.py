import logging
from typing import Final

from fabricks.context.runtime import IS_DEBUGMODE, LOGLEVEL
from fabricks.utils.log import get_logger

logger, _ = get_logger("logs", LOGLEVEL, table=None, debugmode=IS_DEBUGMODE)

DEFAULT_LOGGER: Final[logging.Logger] = logger
