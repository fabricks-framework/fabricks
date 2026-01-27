import os
import pathlib

from fabricks.models import Database, StepBronzeConf, StepGoldConf, StepSilverConf
from fabricks.utils.path import Path


def get_config_from_file():
    path = pathlib.Path(os.getcwd())

    while path is not None:
        if (path / "fabricksconfig.json").exists():
            break
        if (path / "pyproject.toml").exists():
            break
        if path == path.parent:
            break

        path = path.parent

    if (path / "fabricksconfig.json").exists():
        import json

        with open((path / "fabricksconfig.json"), "r") as f:
            config = json.load(f)
            return path, config, "json"

    if (path / "pyproject.toml").exists():
        import sys

        if sys.version_info >= (3, 11):
            import tomllib
        else:
            import tomli as tomllib  # type: ignore

        with open((path / "pyproject.toml"), "rb") as f:
            config = tomllib.load(f)
            return path, config.get("tool", {}).get("fabricks", {}), "pyproject"

    return None, {}, None


def get_storage_paths(
    objects: list[StepBronzeConf] | list[StepSilverConf] | list[StepGoldConf] | list[Database],
    variables: dict,
) -> dict:
    d = {}
    for o in objects:
        d[o.name] = Path.from_uri(o.path_options.storage, regex=variables)
    return d


def get_runtime_path(
    objects: list[StepBronzeConf] | list[StepSilverConf] | list[StepGoldConf],
    root: Path,
) -> dict:
    d = {}
    for o in objects:
        d[o.name] = root.joinpath(o.path_options.runtime)
    return d
