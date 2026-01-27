import logging
from typing import Final

from fabricks.models.config import ConfigOptions
from fabricks.utils.log import get_logger
from fabricks.utils.path import Path

logger, _ = get_logger("logs", level=logging.DEBUG)
config = ConfigOptions()

# Constants
CONFIG: Final[ConfigOptions] = config

PATH_CONFIG: Final[Path] = Path(config.config, assume_git=True)
PATH_RUNTIME: Final[Path] = Path(config.runtime, assume_git=True)
PATH_NOTEBOOKS: Final[Path] = Path(config.notebooks, assume_git=True)
IS_JOB_CONFIG_FROM_YAML: Final[bool] = config.job_config_from_yaml
IS_DEBUGMODE: Final[bool] = config.debugmode
IS_DEVMODE: Final[bool] = config.devmode
IS_FUNMODE: Final[bool] = config.funmode
LOGLEVEL: Final[int] = config.loglevel
