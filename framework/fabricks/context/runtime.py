from typing import Final, Optional
from zoneinfo import ZoneInfo

import yaml

from fabricks.context.config import PATH_CONFIG
from fabricks.models import Database, RuntimeConf, StepBronzeConf, StepGoldConf, StepSilverConf
from fabricks.utils.path import FileSharePath, GitPath

with open(str(PATH_CONFIG)) as f:
    data = yaml.safe_load(f)

conf = [d["conf"] for d in data][0]
assert conf, "conf mandatory"

CONF_RUNTIME: Final[RuntimeConf] = RuntimeConf.model_validate(conf)

BRONZE: dict[str, StepBronzeConf] = {s.name: s for s in CONF_RUNTIME.bronze or []}
SILVER: dict[str, StepSilverConf] = {s.name: s for s in CONF_RUNTIME.silver or []}
GOLD: dict[str, StepGoldConf] = {g.name: g for g in CONF_RUNTIME.gold or []}
STEPS = {**BRONZE, **SILVER, **GOLD}

databases: list[Database] = CONF_RUNTIME.databases or []
credentials = CONF_RUNTIME.credentials or {}
variables = CONF_RUNTIME.variables or {}
VARIABLES: dict = variables


IS_UNITY_CATALOG: Final[bool] = CONF_RUNTIME.options.unity_catalog or False
CATALOG: Optional[str] = CONF_RUNTIME.options.catalog

if IS_UNITY_CATALOG and not CATALOG:
    raise ValueError("catalog mandatory in options if unity catalog is enabled")

SECRET_SCOPE: Final[str] = CONF_RUNTIME.options.secret_scope

TIMEZONE: Final[ZoneInfo] = ZoneInfo(CONF_RUNTIME.options.timezone)

IS_TYPE_WIDENING: Final[bool] = CONF_RUNTIME.options.type_widening or False

# Resolve all paths at once
PATHS_RESOLVED = CONF_RUNTIME.resolved_path_options

FABRICKS_STORAGE: Final[FileSharePath] = PATHS_RESOLVED.storage

FABRICKS_STORAGE_CREDENTIAL: Final[Optional[str]] = CONF_RUNTIME.path_options.storage_credential

PATH_UDFS: Final[GitPath] = PATHS_RESOLVED.udfs
PATH_PARSERS: Final[GitPath] = PATHS_RESOLVED.parsers
PATH_EXTENDERS: Final[GitPath] = PATHS_RESOLVED.extenders
PATH_VIEWS: Final[GitPath] = PATHS_RESOLVED.views
PATH_SCHEDULES: Final[GitPath] = PATHS_RESOLVED.schedules
PATH_REQUIREMENTS: Final[GitPath] = PATHS_RESOLVED.requirements
PATH_MASKS: Final[GitPath] = PATHS_RESOLVED.masks
PATHS_STORAGE: Final[dict[str, FileSharePath]] = PATHS_RESOLVED.storages
PATHS_RUNTIME: Final[dict[str, GitPath]] = PATHS_RESOLVED.runtimes

Bronzes = list(BRONZE.keys())
Silvers = list(SILVER.keys())
Golds = list(GOLD.keys())
Steps = Bronzes + Silvers + Golds
