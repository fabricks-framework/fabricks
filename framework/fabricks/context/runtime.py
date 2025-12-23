from typing import Final, List, Optional

import yaml

from fabricks.context.config import PATH_CONFIG, PATH_RUNTIME
from fabricks.context.helpers import get_runtime_path, get_storage_paths
from fabricks.models import BronzeConf, Database, GoldConf, RuntimeConf, SilverConf
from fabricks.utils.path import Path

with open(str(PATH_CONFIG)) as f:
    data = yaml.safe_load(f)

conf_data = [d["conf"] for d in data][0]
assert conf_data, "conf mandatory"
CONF_RUNTIME: Final[RuntimeConf] = RuntimeConf.model_validate(conf_data)

BRONZE: List[BronzeConf] = CONF_RUNTIME.bronze or []
SILVER: List[SilverConf] = CONF_RUNTIME.silver or []
GOLD: List[GoldConf] = CONF_RUNTIME.gold or []
STEPS = BRONZE + SILVER + GOLD

databases: List[Database] = CONF_RUNTIME.databases or []
credentials = CONF_RUNTIME.credentials or []
variables = CONF_RUNTIME.variables or {}
VARIABLES: dict = variables

IS_UNITY_CATALOG: Final[bool] = CONF_RUNTIME.options.unity_catalog or False
CATALOG: Optional[str] = CONF_RUNTIME.options.catalog

if IS_UNITY_CATALOG and not CATALOG:
    raise ValueError("catalog mandatory in options if unity catalog is enabled")

SECRET_SCOPE: Final[str] = CONF_RUNTIME.options.secret_scope

TIMEZONE: Final[Optional[str]] = CONF_RUNTIME.options.timezone

IS_TYPE_WIDENING: Final[bool] = (
    CONF_RUNTIME.options.type_widening if CONF_RUNTIME.options.type_widening is not None else True
)

FABRICKS_STORAGE: Final[Path] = Path.from_uri(CONF_RUNTIME.path_options.storage, regex=variables)

FABRICKS_STORAGE_CREDENTIAL: Final[Optional[str]] = CONF_RUNTIME.path_options.storage_credential

PATH_UDFS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.udfs)

PATH_PARSERS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.parsers)

PATH_EXTENDERS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.extenders or "fabricks/extenders")

PATH_VIEWS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.views)

PATH_SCHEDULES: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.schedules)

PATH_REQUIREMENTS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.requirements)

PATH_MASKS: Final[Path] = PATH_RUNTIME.joinpath(CONF_RUNTIME.path_options.masks or "fabricks/masks")

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

Bronzes = [b.name for b in BRONZE]
Silvers = [s.name for s in SILVER]
Golds = [g.name for g in GOLD]
Steps = Bronzes + Silvers + Golds
