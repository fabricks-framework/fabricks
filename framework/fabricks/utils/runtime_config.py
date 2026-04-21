from pathlib import Path
from typing import Any

import yaml


def _as_variables(data: Any, source: str) -> dict[str, Any]:
    if data is None:
        return {}

    if isinstance(data, dict):
        variables = data.get("variables", data)
        if not isinstance(variables, dict):
            raise ValueError(f"variables in {source} must be a mapping")
        return dict(variables)

    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
        return _as_variables(data[0], source=source)

    raise ValueError(f"variables file {source} must contain a mapping")


def _resolve_variables_path(
    conf_data: dict[str, Any],
    config_path: Path,
    external_variables_file: str | None,
) -> Path | None:
    variables_file = external_variables_file or conf_data.get("variables_file")
    if not variables_file or str(variables_file).lower() == "none":
        return None

    path = Path(str(variables_file))
    if path.is_absolute():
        return path

    return config_path.parent / path


def _build_variable_lookup(variables: dict[str, Any]) -> dict[str, Any]:
    lookup: dict[str, Any] = {}
    for key, value in variables.items():
        key_string = str(key)
        lookup[key_string] = value

        normalized = key_string.lstrip("\\")
        if normalized != key_string:
            lookup[normalized] = value

    return lookup


def _substitute_value(value: Any, lookup: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {k: _substitute_value(v, lookup) for k, v in value.items()}

    if isinstance(value, list):
        return [_substitute_value(item, lookup) for item in value]

    if not isinstance(value, str):
        return value

    if value in lookup:
        return lookup[value]

    new_value = value
    for key, variable_value in sorted(lookup.items(), key=lambda item: len(item[0]), reverse=True):
        new_value = new_value.replace(key, str(variable_value))

    return new_value


def prepare_runtime_conf_data(
    conf_data: dict[str, Any],
    config_path: Path,
    external_variables_file: str | None = None,
) -> dict[str, Any]:
    prepared = dict(conf_data)
    base_variables = _as_variables(prepared.get("variables"), source="runtime config")
    variables_path = _resolve_variables_path(
        conf_data=prepared,
        config_path=config_path,
        external_variables_file=external_variables_file,
    )

    file_variables: dict[str, Any] = {}
    if variables_path:
        with open(variables_path, encoding="utf-8") as f:
            file_variables = _as_variables(yaml.safe_load(f), source=str(variables_path))

    merged_variables = {**base_variables, **file_variables}
    prepared["variables"] = merged_variables
    prepared.pop("variables_file", None)

    if not merged_variables:
        return prepared

    return _substitute_value(prepared, _build_variable_lookup(merged_variables))
