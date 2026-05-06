"""Shared variable substitution utilities."""

import re
from functools import lru_cache
from typing import Any

_DOLLAR_VAR_PATTERN = re.compile(r"\$[A-Za-z0-9_-]+")


@lru_cache(maxsize=8)
def _build_variable_lookup_cached(items: tuple[tuple[str, Any], ...]) -> dict[str, Any]:
    """Build a lookup dictionary for variable substitution (cached internal implementation)."""
    lookup: dict[str, Any] = {}
    for key, value in items:
        key_string = str(key)
        lookup[key_string] = value

        normalized = key_string.lstrip("\\")
        if normalized != key_string:
            lookup[normalized] = value

    return lookup


def build_variable_lookup(variables: dict[str, Any]) -> dict[str, Any]:
    """Build a lookup dictionary for variable substitution."""
    # Convert dict to hashable tuple of items for caching
    items = tuple(sorted(variables.items()))
    return _build_variable_lookup_cached(items)


def substitute_value(value: Any, lookup: dict[str, Any], strict: bool = False) -> Any:
    """
    Recursively substitute variables in values.

    Args:
        value: The value to substitute variables in
        lookup: Dictionary mapping variable names to their values
        strict: If True, raise ValueError for missing variables

    Raises:
        ValueError: If strict=True and a variable is not found in lookup
    """
    if isinstance(value, dict):
        return {k: substitute_value(v, lookup, strict) for k, v in value.items()}

    if isinstance(value, list):
        return [substitute_value(item, lookup, strict) for item in value]

    if not isinstance(value, str):
        return value

    # Early exit for strings without variables (most common case)
    if "$" not in value:
        return value

    # Check for exact match (whole value is a variable)
    if value in lookup:
        return lookup[value]

    # Perform regex substitution with single-pass validation
    if strict:
        missing_vars = []

        def _substitute(match):
            var_name = match.group(0)
            if var_name not in lookup:
                missing_vars.append(var_name)
                return var_name
            return str(lookup[var_name])

        result = _DOLLAR_VAR_PATTERN.sub(_substitute, value)

        if missing_vars:
            raise ValueError(f"Variable(s) not found in lookup: {', '.join(missing_vars)}")

        return result

    else:
        return _DOLLAR_VAR_PATTERN.sub(
            lambda match: str(lookup.get(match.group(0), match.group(0))),
            value,
        )
