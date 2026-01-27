from typing import Final, Optional

import yaml

from fabricks.context.config import PATH_CONFIG
from fabricks.models import Database, RuntimeConf, StepBronzeConf, StepGoldConf, StepSilverConf
from fabricks.utils.path import Path

with open(str(PATH_CONFIG)) as f:
    data = yaml.safe_load(f)

conf_data = [d["conf"] for d in data][0]
assert conf_data, "conf mandatory"
CONF_RUNTIME: Final[RuntimeConf] = RuntimeConf.model_validate(conf_data)

BRONZE: list[StepBronzeConf] = CONF_RUNTIME.bronze or []
SILVER: list[StepSilverConf] = CONF_RUNTIME.silver or []
GOLD: list[StepGoldConf] = CONF_RUNTIME.gold or []
STEPS = BRONZE + SILVER + GOLD

databases: list[Database] = CONF_RUNTIME.databases or []
credentials = CONF_RUNTIME.credentials or []
variables = CONF_RUNTIME.variables or {}
VARIABLES: dict = variables


IS_UNITY_CATALOG: Final[bool] = CONF_RUNTIME.options.unity_catalog or False
CATALOG: Optional[str] = CONF_RUNTIME.options.catalog

if IS_UNITY_CATALOG and not CATALOG:
    raise ValueError("catalog mandatory in options if unity catalog is enabled")

SECRET_SCOPE: Final[str] = CONF_RUNTIME.options.secret_scope

TIMEZONE: Final[Optional[str]] = CONF_RUNTIME.options.timezone

IS_TYPE_WIDENING: Final[bool] = CONF_RUNTIME.options.type_widening or False

# Resolve all paths at once
PATHS_RESOLVED = CONF_RUNTIME.resolved_path_options

FABRICKS_STORAGE: Final[Path] = PATHS_RESOLVED.storage

FABRICKS_STORAGE_CREDENTIAL: Final[Optional[str]] = CONF_RUNTIME.path_options.storage_credential

PATH_UDFS: Final[Path] = PATHS_RESOLVED.udfs
PATH_PARSERS: Final[Path] = PATHS_RESOLVED.parsers
PATH_EXTENDERS: Final[Path] = PATHS_RESOLVED.extenders
PATH_VIEWS: Final[Path] = PATHS_RESOLVED.views
PATH_SCHEDULES: Final[Path] = PATHS_RESOLVED.schedules
PATH_REQUIREMENTS: Final[Path] = PATHS_RESOLVED.requirements
PATH_MASKS: Final[Path] = PATHS_RESOLVED.masks
PATHS_STORAGE: Final[dict[str, Path]] = PATHS_RESOLVED.storage_paths
PATHS_RUNTIME: Final[dict[str, Path]] = PATHS_RESOLVED.runtime_paths

Bronzes = [b.name for b in BRONZE]
Silvers = [s.name for s in SILVER]
Golds = [g.name for g in GOLD]
Steps = Bronzes + Silvers + Golds
