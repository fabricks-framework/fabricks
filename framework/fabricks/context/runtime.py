import os
import sys
from typing import Final, List, Optional

import yaml
from databricks.sdk.runtime import spark

from fabricks.utils.path import Path


def get_config_from_toml():
    import os
    import pathlib
    import sys

    if sys.version_info >= (3, 11):
        import tomllib
    else:
        import tomli as tomllib  # type: ignore

    path = pathlib.Path(os.getcwd())
    while path is not None and not (path / "pyproject.toml").exists():
        path = path.parent

    if (path / "pyproject.toml").exists():
        with open((path / "pyproject.toml"), "rb") as f:
            config = tomllib.load(f)
            return path, config.get("tool", {}).get("fabricks", {})

    return None, {}


try:
    pyproject_path, fabricks_cfg = get_config_from_toml()

    if runtime := fabricks_cfg.get("runtime"):
        assert pyproject_path is not None  # Cannot be null since we got the config from it
        runtime = pyproject_path.joinpath(runtime)  # Must resolve relative to pyproject.toml
    else:
        runtime = os.environ.get("FABRICKS_RUNTIME")

    if runtime is None and pyproject_path is not None:
        runtime = pyproject_path
    elif runtime is None:
        raise ValueError("Must have at least a pyproject.toml or set FABRICKS_RUNTIME")

    path_runtime = Path(runtime, assume_git=True)
    assert path_runtime, "runtime mandatory in cluster config"
    PATH_RUNTIME: Final[Path] = path_runtime

    if notebooks := fabricks_cfg.get("notebooks"):
        assert pyproject_path is not None  # Cannot be null since we got the config from it
        notebooks = pyproject_path.joinpath(notebooks)  # Must resolve relative to pyproject.toml
    else:
        notebooks = os.environ.get("FABRICKS_NOTEBOOKS")

    notebooks = notebooks if notebooks else path_runtime.join("notebooks")
    assert notebooks, "notebooks mandatory"
    PATH_NOTEBOOKS: Final[Path] = Path(str(notebooks), assume_git=True)

    PATH_LIBRARIES = "/dbfs/mnt/fabricks/site-packages"

    spark._sc._python_includes.append(PATH_LIBRARIES)  # type: ignore
    sys.path.append(PATH_LIBRARIES)

    IS_TEST: Final[bool] = os.environ.get("FABRICKS_IS_TEST") in ("TRUE", "true", "True", "1")
    IS_LIVE: Final[bool] = os.environ.get("FABRICKS_IS_LIVE") in ("TRUE", "true", "True", "1")

    config_path = fabricks_cfg.get("config")
    if not config_path:
        config_path = PATH_RUNTIME.join(
            "fabricks",
            f"conf.{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId')}.yml",
        ).string
    else:
        assert pyproject_path is not None  # Cannot be null since we got the config from it
        config_path = pyproject_path.joinpath(config_path)

    PATH_CONFIG: Final[Path] = Path(config_path, assume_git=True)

    with open(config_path) as f:
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
