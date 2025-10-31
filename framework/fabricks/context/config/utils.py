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
        if path == path.parent:
            break
        path = path.parent

    if (path / "pyproject.toml").exists():
        with open((path / "pyproject.toml"), "rb") as f:
            config = tomllib.load(f)
            return path, config.get("tool", {}).get("fabricks", {})

    return None, {}


def get_config_from_json():
    import json
    import os
    import pathlib

    path = pathlib.Path(os.getcwd())
    while path is not None and not (path / "fabricksconfig.json").exists():
        if path == path.parent:
            break
        path = path.parent

    if (path / "fabricksconfig.json").exists():
        with open((path / "fabricksconfig.json"), "r") as f:
            config = json.load(f)
            return path, config

    return None, {}


def get_config_from_file():
    json_path, json_config = get_config_from_json()
    if json_config:
        return json_path, json_config

    pyproject_path, pyproject_config = get_config_from_toml()
    if pyproject_config:
        return pyproject_path, pyproject_config

    return None, {}
