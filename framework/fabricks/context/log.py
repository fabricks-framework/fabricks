import logging
from typing import Final

from fabricks.utils.log import get_logger

logger, _ = get_logger("logs", logging.DEBUG, table=None)

DEFAULT_LOGGER: Final[logging.Logger] = logger
