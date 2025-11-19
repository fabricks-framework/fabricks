from typing import Final, Optional

import yaml

from fabricks.context.config import PATH_CONFIG, PATH_RUNTIME
from fabricks.context.helpers import get_runtime_path, get_storage_paths
from fabricks.utils.path import Path

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
