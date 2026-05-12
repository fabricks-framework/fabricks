"""Utility functions for runtime configuration parsing and transformation."""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from fabricks.utils.path import FileSharePath, GitPath, resolve_fileshare_path, resolve_git_path
from fabricks.utils.variables import build_variable_lookup, substitute_value


def _as_variables(data: Any, source: str) -> dict[str, Any]:
    """Extract variables dictionary from various data structures."""
    if data is None:
        return {}

    if isinstance(data, dict):
        variables = data.get("variables", data)
        if not isinstance(variables, dict):
            raise ValueError(f"variables in {source} must be a mapping")

        return dict(variables)

    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
        return _as_variables(data[0], source=source)

    if isinstance(data, str):
        # String is treated as a file path - handled by caller
        return {}

    raise ValueError(f"variables file {source} must contain a mapping")


def _resolve_variables_path(
    conf_data: dict[str, Any],
    config_path: Path,
    external_variables_file: str | None,
) -> Path | None:
    """Resolve the path to a variables file from config data."""
    variables_file = external_variables_file or conf_data.get("variables_file")
    if not variables_file or str(variables_file).lower() == "none":
        return None

    path = Path(str(variables_file))
    if path.is_absolute():
        return path

    return config_path.parent / path


@lru_cache(maxsize=128)
def _load_variables_from_file_cached(variables_path_str: str) -> dict[str, Any]:
    """Cached YAML file loader (internal)."""
    variables_path = Path(variables_path_str)
    with open(variables_path, encoding="utf-8") as f:
        return _as_variables(yaml.safe_load(f), source=variables_path_str)


def load_variables_from_file(variables_path: Path, config_path: Path) -> dict[str, Any]:
    """Load variables from a YAML file with proper error context (cached)."""
    try:
        return _load_variables_from_file_cached(str(variables_path))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"variables file '{variables_path}' referenced by config '{config_path}' was not found",
        ) from exc


def load_variables(
    data: dict[str, Any],
    config_path: Path,
    variables_path: str | None = None,
) -> dict[str, Any]:
    """
    Load variables from external file or inline dict.

    Priority order (first non-empty wins):
    1. variables_path (from FABRICKS_VARIABLE or path_options.variables)
    2. inline variables dict

    Args:
        data: Raw config dictionary
        config_path: Path to the config file
        variables_path: Path to variables file (already resolved from env var or config)

    Returns:
        Resolved variables dictionary
    """
    inline_variables = data.get("variables")

    if variables_path:
        file_path = Path(variables_path)
        if not file_path.is_absolute():
            file_path = config_path.parent / file_path

        return load_variables_from_file(file_path, config_path)

    if inline_variables:
        return _as_variables(inline_variables, source="runtime config")

    return {}


def perform_variable_substitution(
    data: dict[str, Any],
    variables: dict[str, Any],
) -> dict[str, Any]:
    """
    Perform variable substitution on runtime config data.

    Args:
        data: Raw config dictionary
        variables: Variables dictionary for substitution

    Returns:
        Config dictionary with variables substituted
    """
    if not variables:
        return data

    prepared = dict(data)
    prepared["variables"] = variables
    return substitute_value(prepared, build_variable_lookup(variables))


def resolve_runtime_paths(
    path_options: dict[str, Any],
    variables: dict[str, Any] | None,
    bronze: list[Any] | None,
    silver: list[Any] | None,
    gold: list[Any] | None,
    databases: list[Any] | None,
    base_runtime: GitPath,
) -> dict[str, Any]:
    """
    Resolve all runtime paths to Path objects.

    Note: Variable substitution has already been performed on path_options
    before this function is called, so paths are already resolved.

    Args:
        path_options: Runtime path configuration (already variable-substituted)
        variables: Runtime variables (kept for backward compatibility, not used)
        bronze: Bronze step configurations
        silver: Silver step configurations
        gold: Gold step configurations
        databases: Database configurations
        base_runtime: Base runtime GitPath

    Returns:
        Dictionary with resolved storage and runtime paths
    """
    # Collect storage paths (variables already substituted in path_options)
    storage_paths: dict[str, FileSharePath] = {
        "fabricks": resolve_fileshare_path(path_options["storage"]),
    }

    # Add storage paths for bronze/silver/gold/databases
    for objects in [bronze, silver, gold, databases]:
        if objects:
            for obj in objects:
                storage_paths[obj.name] = resolve_fileshare_path(obj.path_options.storage)

    # Collect runtime paths
    runtime_paths: dict[str, GitPath] = {}
    for objects in [bronze, silver, gold]:
        if objects:
            for obj in objects:
                runtime_paths[obj.name] = resolve_git_path(
                    obj.path_options.runtime,
                    base=base_runtime,
                )

    return {
        "storage": storage_paths["fabricks"],
        "udfs": resolve_git_path(path=path_options["udfs"], base=base_runtime),
        "parsers": resolve_git_path(path=path_options["parsers"], base=base_runtime),
        "schedules": resolve_git_path(path=path_options["schedules"], base=base_runtime),
        "views": resolve_git_path(path=path_options["views"], base=base_runtime),
        "requirements": resolve_git_path(path=path_options["requirements"], base=base_runtime),
        "extenders": resolve_git_path(
            path=path_options.get("extenders"),
            base=base_runtime,
            default="fabricks/extenders",
        ),
        "masks": resolve_git_path(
            path=path_options.get("masks"),
            base=base_runtime,
            default="fabricks/masks",
        ),
        "storages": storage_paths,
        "runtimes": runtime_paths,
    }
