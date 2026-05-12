from functools import lru_cache
from typing import Any, Iterable, Optional, cast

import yaml

from fabricks.utils.path.base import BasePath
from fabricks.utils.variables import build_variable_lookup, substitute_value


@lru_cache(maxsize=128)
def _read_yaml_cached(file: str) -> list[dict]:
    """Cache YAML file reads with LRU eviction. Max 128 unique file paths cached."""
    with open(file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_yaml(
    path: BasePath,
    root: Optional[str] = None,
    preferred_file_name: Optional[str] = None,
    variables: Optional[dict[str, Any]] = None,
    strict: bool = True,
) -> Iterable[dict]:
    """
    Read YAML files from a path with optional variable substitution.

    Args:
        path: The path to search for YAML files
        root: Optional root key to extract from each document
        preferred_file_name: Optional preferred file name pattern
        variables: Optional dictionary of variables for substitution
        strict: If True, raise ValueError when variables are not found (default: True)

    Yields:
        Dictionary data from YAML files, with variables substituted if provided

    Raises:
        ValueError: If strict=True and a variable is not found in the variables dict
    """
    found = False
    lookup = build_variable_lookup(variables) if variables else None

    for file in path.walk():
        if not file.endswith(".yml"):
            continue

        if preferred_file_name is not None and preferred_file_name not in file:
            continue

        found = True

        data = _read_yaml_cached(file)
        for job_config in data:
            if root:
                config = cast(dict, job_config[root])
            else:
                config = cast(dict, job_config)

            if lookup:
                config = substitute_value(config, lookup, strict=strict)

            yield config

    if preferred_file_name is not None and not found:
        yield from read_yaml(
            path=path,
            root=root,
            preferred_file_name=None,
            variables=variables,
            strict=strict,
        )
