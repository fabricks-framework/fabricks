import logging
import os
from typing import Final

from fabricks.context.helpers import get_config_from_file
from fabricks.utils.log import get_logger
from fabricks.utils.path import Path
from fabricks.utils.spark import spark

logger, _ = get_logger("logs", level=logging.DEBUG)
file_path, file_config, origin = get_config_from_file()

if file_path:
    logger.debug(f"found {origin} config ({file_path})", extra={"label": "config"})

# path to runtime
runtime = os.environ.get("FABRICKS_RUNTIME", "none")
runtime = None if runtime.lower() == "none" else runtime
if runtime is None:
    if runtime := file_config.get("runtime"):
        assert file_path is not None
        runtime = file_path.joinpath(runtime)
        logger.debug(f"resolve runtime from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve runtime from env", extra={"label": "config"})

if runtime is None:
    if file_path is not None:
        runtime = file_path
        logger.debug(f"resolve runtime from {origin} file", extra={"label": "config"})
    else:
        raise ValueError(
            "could not resolve runtime (could not find pyproject.toml nor fabricksconfig.json nor FABRICKS_RUNTIME)"
        )

path_runtime = Path(runtime, assume_git=True)

# path to config
config = os.environ.get("FABRICKS_CONFIG")
if config is None:
    if config := file_config.get("config"):
        assert file_path is not None
        config = file_path.joinpath(config)
        logger.debug(f"resolve config from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve config from env", extra={"label": "config"})

if config is None:
    logger.debug("resolve config from default path", extra={"label": "config"})
    config = path_runtime.joinpath(
        "fabricks",
        f"conf.{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId')}.yml",
    ).string

path_config = Path(config, assume_git=True)

# path to notebooks
notebooks = os.environ.get("FABRICKS_NOTEBOOKS", "none")
notebooks = None if notebooks.lower() == "none" else notebooks
if notebooks is None:
    if notebooks := file_config.get("notebooks"):
        assert file_path is not None
        notebooks = file_path.joinpath(notebooks)
        logger.debug(f"resolve notebooks from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve notebooks from env", extra={"label": "config"})

if notebooks is None:
    logger.debug("resolve notebooks from default path", extra={"label": "config"})
    notebooks = path_runtime.joinpath("notebooks")

path_notebooks = Path(str(notebooks), assume_git=True)

# job config from yaml
is_job_config_from_yaml = os.environ.get("FABRICKS_IS_JOB_CONFIG_FROM_YAML", None)
if is_job_config_from_yaml is None:
    if is_job_config_from_yaml := file_config.get("job_config_from_yaml"):
        logger.debug(f"resolve job_config_from_yaml from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve job_config_from_yaml from env", extra={"label": "config"})

# debug mode
is_debugmode = os.environ.get("FABRICKS_IS_DEBUGMODE", None)
if is_debugmode is None:
    if is_debugmode := file_config.get("debugmode"):
        logger.debug(f"resolve debugmode from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve debugmode from env", extra={"label": "config"})

# dev mode
is_devmode = os.environ.get("FABRICKS_IS_DEVMODE", None)
if is_devmode is None:
    if is_devmode := file_config.get("devmode"):
        logger.debug(f"resolve devmode from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve devmode from env", extra={"label": "config"})

# log level
loglevel = os.environ.get("FABRICKS_LOGLEVEL", None)
if loglevel is None:
    if loglevel := file_config.get("loglevel"):
        logger.debug(f"resolve loglevel from {origin} file", extra={"label": "config"})
else:
    logger.debug("resolve loglevel from env", extra={"label": "config"})

loglevel = loglevel.upper() if loglevel else "INFO"
if loglevel == "DEBUG":
    _loglevel = logging.DEBUG
elif loglevel == "INFO":
    _loglevel = logging.INFO
elif loglevel == "WARNING":
    _loglevel = logging.WARNING
elif loglevel == "ERROR":
    _loglevel = logging.ERROR
elif loglevel == "CRITICAL":
    _loglevel = logging.CRITICAL
else:
    raise ValueError(f"could not resolve {loglevel} (DEBUG, INFO, WARNING, ERROR or CRITICAL)")

# Constants
PATH_CONFIG: Final[Path] = path_config
PATH_RUNTIME: Final[Path] = path_runtime
PATH_NOTEBOOKS: Final[Path] = path_notebooks
IS_JOB_CONFIG_FROM_YAML: Final[bool] = str(is_job_config_from_yaml).lower() in ("true", "1", "yes")
IS_DEBUGMODE: Final[bool] = str(is_debugmode).lower() in ("true", "1", "yes")
IS_DEVMODE: Final[bool] = str(is_devmode).lower() in ("true", "1", "yes")
LOGLEVEL: Final[int] = _loglevel
