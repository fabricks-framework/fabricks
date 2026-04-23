import re
from pathlib import Path
from typing import Any


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

    if "$" not in value:
        return value

    return re.sub(
        r"\$[A-Za-z0-9_-]+",
        lambda match: str(lookup.get(match.group(0), match.group(0))),
        value,
    )
