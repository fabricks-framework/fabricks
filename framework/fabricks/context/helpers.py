import os
import pathlib
from typing import List

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


def get_storage_paths(objects: List[dict], variables: dict) -> dict:
    d = {}
    for o in objects:
        if o:
            name = o.get("name")
            assert name
            uri = o.get("path_options", {}).get("storage")
            assert uri
            d[name] = Path.from_uri(uri, regex=variables)
    return d


def get_runtime_path(objects: List[dict], root: Path) -> dict:
    d = {}
    for o in objects:
        name = o.get("name")
        assert name
        uri = o.get("path_options", {}).get("runtime")
        assert uri
        d[name] = root.joinpath(uri)
    return d
