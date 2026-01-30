from typing import Final

from fabricks.models.config import ConfigOptions
from fabricks.utils.path import GitPath

config = ConfigOptions()

# Constants
CONFIG: Final[ConfigOptions] = config

PATH_CONFIG: Final[GitPath] = config.resolved_paths.config
PATH_RUNTIME: Final[GitPath] = config.resolved_paths.runtime
PATH_NOTEBOOKS: Final[GitPath] = config.resolved_paths.notebooks
IS_JOB_CONFIG_FROM_YAML: Final[bool] = config.job_config_from_yaml
IS_DEBUGMODE: Final[bool] = config.debugmode
IS_DEVMODE: Final[bool] = config.devmode
IS_FUNMODE: Final[bool] = config.funmode
LOGLEVEL: Final[int] = config.loglevel
