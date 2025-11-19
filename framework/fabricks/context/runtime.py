import logging
import os
from typing import Final, Optional

import yaml

from fabricks.context.helpers import get_config_from_file, get_runtime_path, get_storage_paths
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

LOGLEVEL: Final[int] = _loglevel

path_config = os.environ.get("FABRICKS_CONFIG")
if path_config is None:
    if path_config := file_config.get("config"):
        assert file_path is not None
        path_config = file_path.joinpath(path_config)

if path_config is None:
    path_config = PATH_RUNTIME.joinpath(
        "fabricks",
        f"conf.{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId')}.yml",
    ).string

PATH_CONFIG: Final[Path] = Path(path_config, assume_git=True)

with open(str(PATH_CONFIG)) as f:
    data = yaml.safe_load(f)

conf: dict = [d["conf"] for d in data][0]
assert conf, "conf mandatory"
CONF_RUNTIME: Final[dict] = conf

BRONZE = CONF_RUNTIME.get("bronze", [{}])
SILVER = CONF_RUNTIME.get("silver", [{}])
GOLD = CONF_RUNTIME.get("gold", [{}])
STEPS = BRONZE + SILVER + GOLD

databases = CONF_RUNTIME.get("databases", [{}])
credentials = CONF_RUNTIME.get("credentials", {})
variables = CONF_RUNTIME.get("variables", {})
VARIABLES: dict = variables

conf_options = CONF_RUNTIME.get("options", {})
assert conf_options, "options mandatory"

IS_UNITY_CATALOG: Final[bool] = str(conf_options.get("unity_catalog", "False")).lower() in ("true", "1", "yes")
CATALOG: Optional[str] = conf_options.get("catalog")

if IS_UNITY_CATALOG and not CATALOG:
    raise ValueError("catalog mandatory in options when unity_catalog is enabled")

secret_scope = conf_options.get("secret_scope")
assert secret_scope, "secret_scope mandatory in options"
SECRET_SCOPE: Final[str] = secret_scope

timezone = conf_options.get("timezone")
TIMEZONE: Final[str] = timezone

IS_TYPE_WIDENING: Final[bool] = str(conf_options.get("type_widening", "True")).lower() in ("true", "1", "yes")

path_options = CONF_RUNTIME.get("path_options", {})
assert path_options, "options mandatory"

fabricks_uri = path_options.get("storage")
assert fabricks_uri, "storage mandatory in path options"
FABRICKS_STORAGE: Final[Path] = Path.from_uri(fabricks_uri, regex=variables)

FABRICKS_STORAGE_CREDENTIAL: Final[Optional[str]] = path_options.get("storage_credential")

path_udfs = path_options.get("udfs", "fabricks/udfs")
assert path_udfs, "path to udfs mandatory"
PATH_UDFS: Final[Path] = PATH_RUNTIME.joinpath(path_udfs)

path_parsers = path_options.get("parsers", "fabricks/parsers")
assert path_parsers, "path to parsers mandatory"
PATH_PARSERS: Final[Path] = PATH_RUNTIME.joinpath(path_parsers)

path_extenders = path_options.get("extenders", "fabricks/extenders")
assert path_extenders, "path to extenders mandatory"
PATH_EXTENDERS: Final[Path] = PATH_RUNTIME.joinpath(path_extenders)

path_views = path_options.get("views", "fabricks/views")
assert path_views, "path to views mandatory"
PATH_VIEWS: Final[Path] = PATH_RUNTIME.joinpath(path_views)

path_schedules = path_options.get("schedules", "fabricks/schedules")
assert path_schedules, "path to schedules mandatory"
PATH_SCHEDULES: Final[Path] = PATH_RUNTIME.joinpath(path_schedules)

path_requirements = path_options.get("requirements", "fabricks/requirements")
assert path_requirements, "path to requirements mandatory"
PATH_REQUIREMENTS: Final[Path] = PATH_RUNTIME.joinpath(path_requirements)

path_masks = path_options.get("masks", "fabricks/masks")
assert path_masks, "path to masks mandatory"
PATH_MASKS: Final[Path] = PATH_RUNTIME.joinpath(path_masks)

PATHS_STORAGE: Final[dict[str, Path]] = {
    "fabricks": FABRICKS_STORAGE,
    **get_storage_paths(BRONZE, variables),
    **get_storage_paths(SILVER, variables),
    **get_storage_paths(GOLD, variables),
    **get_storage_paths(databases, variables),
}

PATHS_RUNTIME: Final[dict[str, Path]] = {
    **get_runtime_path(BRONZE, PATH_RUNTIME),
    **get_runtime_path(SILVER, PATH_RUNTIME),
    **get_runtime_path(GOLD, PATH_RUNTIME),
}
