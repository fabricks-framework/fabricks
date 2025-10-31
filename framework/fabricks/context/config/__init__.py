import logging
import os
from typing import Final

from fabricks.context.config.utils import get_config_from_file
from fabricks.utils.path import Path
from fabricks.utils.spark import spark

file_path, file_config = get_config_from_file()

runtime = os.environ.get("FABRICKS_RUNTIME", "none")
runtime = None if runtime.lower() == "none" else runtime
if runtime is None:
    if runtime := file_config.get("runtime"):
        assert file_path is not None
        runtime = file_path.joinpath(runtime)

if runtime is None:
    if file_path is not None:
        runtime = file_path
    else:
        raise ValueError(
            "could not resolve runtime (could not find pyproject.toml nor fabricksconfig.json nor FABRICKS_RUNTIME)"
        )

path_runtime = Path(runtime, assume_git=True)
PATH_RUNTIME: Final[Path] = path_runtime

notebooks = os.environ.get("FABRICKS_NOTEBOOKS", "none")
notebooks = None if notebooks.lower() == "none" else notebooks
if notebooks is None:
    if notebooks := file_config.get("notebooks"):
        assert file_path is not None
        notebooks = file_path.joinpath(notebooks)

notebooks = notebooks if notebooks else path_runtime.joinpath("notebooks")
PATH_NOTEBOOKS: Final[Path] = Path(str(notebooks), assume_git=True)

is_job_config_from_yaml = os.environ.get("FABRICKS_IS_JOB_CONFIG_FROM_YAML", None)
if is_job_config_from_yaml is None:
    assert file_path is not None
    is_job_config_from_yaml = file_config.get("job_config_from_yaml")

IS_JOB_CONFIG_FROM_YAML: Final[bool] = str(is_job_config_from_yaml).lower() in ("true", "1", "yes")

is_debugmode = os.environ.get("FABRICKS_IS_DEBUGMODE", None)
if is_debugmode is None:
    is_debugmode = file_config.get("debugmode")

IS_DEBUGMODE: Final[bool] = str(is_debugmode).lower() in ("true", "1", "yes")

is_devmode = os.environ.get("FABRICKS_IS_DEVMODE", None)
if is_devmode is None:
    is_devmode = file_config.get("devmode")

IS_DEVMODE: Final[bool] = str(is_devmode).lower() in ("true", "1", "yes")

loglevel = os.environ.get("FABRICKS_LOGLEVEL", None)
if loglevel is None:
    loglevel = file_config.get("loglevel")

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

LOGLEVEL = _loglevel

path_config = os.environ.get("FABRICKS_CONFIG")
if path_config is None:
    if path_config := file_config.get("config"):
        assert file_path is not None
        path_config = file_path.joinpath(path_config)
else:
    path_config = PATH_RUNTIME.joinpath(path_config).string if path_config else None

if not path_config:
    path_config = PATH_RUNTIME.joinpath(
        "fabricks",
        f"conf.{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId')}.yml",
    ).string

PATH_CONFIG: Final[Path] = Path(path_config, assume_git=True)
