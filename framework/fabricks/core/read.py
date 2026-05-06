"""Core YAML reading utilities with automatic variable substitution from context."""

from typing import Any, Iterable, Optional

from fabricks.context import VARIABLES
from fabricks.utils.path import BasePath
from fabricks.utils.read.read_yaml import read_yaml as _read_yaml


def read_yaml(
    path: BasePath,
    root: Optional[str] = None,
    preferred_file_name: Optional[str] = None,
    strict: bool = True,
) -> Iterable[dict[str, Any]]:
    """
    Read YAML files with automatic variable substitution from runtime context.

    This is a convenience wrapper around fabricks.utils.read.read_yaml that
    automatically passes VARIABLES from the global context.

    Args:
        path: Path to read YAML files from
        root: Optional root key to extract from each YAML document
        preferred_file_name: Optional preferred file name to filter by
        strict: If True, raise error when variables are not found (default: True)

    Yields:
        Dictionaries with variable substitution applied

    Raises:
        ValueError: If strict=True and variables are not found in lookup
    """
    return _read_yaml(
        path=path,
        root=root,
        preferred_file_name=preferred_file_name,
        variables=VARIABLES,
        strict=strict,
    )
