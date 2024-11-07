import os
import sys
from typing import Final, List, Optional

import yaml
from databricks.sdk.runtime import spark

from fabricks.utils.path import Path

try:
    runtime = Path(os.environ["FABRICKS_RUNTIME"], assume_git=True)
    assert runtime, "runtime mandatory in cluster config"
    PATH_RUNTIME: Final[Path] = runtime

    notebooks = Path(os.environ["FABRICKS_NOTEBOOKS"], assume_git=True)
    assert notebooks, "notebooks mandatory in cluster config"
    PATH_NOTEBOOKS: Final[Path] = notebooks

    PATH_LIBRARIES = "/dbfs/mnt/fabricks/site-packages"
    spark._sc._python_includes.append(PATH_LIBRARIES)  # type: ignore
    sys.path.append(PATH_LIBRARIES)

    try:
        is_test = os.environ["FABRICKS_IS_TEST"] == "TRUE"
    except Exception:
        is_test = False
    IS_TEST: Final[bool] = is_test

    try:
        is_live = os.environ["FABRICKS_IS_LIVE"] == "TRUE"
    except Exception:
        is_live = False
    IS_LIVE: Final[bool] = is_live

    conf_path = PATH_RUNTIME.join(
        "fabricks",
        f"conf.{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId')}.yml",
    )
    with open(conf_path.string) as f:
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

    secret_scope = conf_options.get("secret_scope")
    assert secret_scope, "secret_scope mandatory in options"
    SECRET_SCOPE: Final[str] = secret_scope

    path_options = CONF_RUNTIME.get("path_options", {})
    assert path_options, "options mandatory"

    fabricks_uri = path_options.get("storage")
    assert fabricks_uri, "storage mandatory in path options"
    FABRICKS_STORAGE: Final[Path] = Path.from_uri(fabricks_uri, regex=variables)

    path_udfs = path_options.get("udfs")
    assert path_udfs, "udfs mandatory in path options"
    PATH_UDFS: Final[Path] = PATH_RUNTIME.join(path_udfs)

    path_parsers = path_options.get("parsers")
    assert path_parsers, "parsers mandatory in path options"
    PATH_PARSERS: Final[Path] = PATH_RUNTIME.join(path_parsers)

    path_extenders = path_options.get("extenders")
    assert path_extenders, "extenders mandatory in path options"
    PATH_EXTENDERS: Final[Path] = PATH_RUNTIME.join(path_extenders)

    path_views = path_options.get("views")
    assert path_views, "views mandatory in path options"
    PATH_VIEWS: Final[Path] = PATH_RUNTIME.join(path_views)

    path_schedules = path_options.get("schedules")
    assert path_schedules, "schedules mandatory in path options"
    PATH_SCHEDULES: Final[Path] = PATH_RUNTIME.join(path_schedules)

    path_requirements = path_options.get("requirements")
    assert path_requirements, "requirements mandatory in path options"
    PATH_REQUIREMENTS: Final[Path] = PATH_RUNTIME.join(path_requirements)

    CATALOG: Optional[str] = CONF_RUNTIME.get("options", {}).get("catalog")

    def _get_storage_paths(objects: List[dict]) -> dict:
        d = {}
        for o in objects:
            if o:
                name = o.get("name")
                assert name
                uri = o.get("path_options", {}).get("storage")
                assert uri
                d[name] = Path.from_uri(uri, regex=variables)
        return d

    PATHS_STORAGE: Final[dict[str, Path]] = {
        "fabricks": FABRICKS_STORAGE,
        **_get_storage_paths(BRONZE),
        **_get_storage_paths(SILVER),
        **_get_storage_paths(GOLD),
        **_get_storage_paths(databases),
    }

    def _get_runtime_path(objects: List[dict]) -> dict:
        d = {}
        for o in objects:
            name = o.get("name")
            assert name
            uri = o.get("path_options", {}).get("runtime")
            assert uri
            d[name] = PATH_RUNTIME.join(uri)
        return d

    PATHS_RUNTIME: Final[dict[str, Path]] = {
        **_get_runtime_path(BRONZE),
        **_get_runtime_path(SILVER),
        **_get_runtime_path(GOLD),
    }

except KeyError as e:
    raise e

except AssertionError as e:
    raise e
