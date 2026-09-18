"""Shared variable substitution utilities.

Variable Substitution:
    Variables are defined with a $ prefix in their keys and referenced with $name.
    Example:
        variables = {"$catalog": "my_catalog"}
        substitute_value("catalog: $catalog", lookup) -> "catalog: my_catalog"

Dollar Escape ($$):
    Use $$ to escape literal $ characters in data (e.g., BC table names).
    Example:
        # BC table name with literal $: "INDUSCABEL$Change Log Entry"
        # In config, escape it as: "INDUSCABEL$$Change Log Entry"
        substitute_value("table: INDUSCABEL$$Change", lookup) -> "table: INDUSCABEL$Change"

    This prevents collisions when:
    - Data contains $ characters (e.g., "Company$G_L Entry")
    - A variable with that name exists (e.g., $G_L is defined)
    - Without $$: "Company$G_L Entry" would incorrectly substitute to "Company<value> Entry"
    - With $$: "Company$$G_L Entry" correctly becomes "Company$G_L Entry"
"""

from functools import lru_cache
import re
from typing import Any

_DOLLAR_VAR_PATTERN = re.compile(r"\$[A-Za-z0-9_-]+")
_DOLLAR_PLACEHOLDER = "\x00ESCAPED_DOLLAR\x00"


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


def substitute_value(
    value: Any,  # noqa: ANN401 - recursive generic value substitution over arbitrary dict/list/scalar config data
    lookup: dict[str, Any],
    strict: bool = False,
) -> Any:  # noqa: ANN401 - see `value` above
    """
    Recursively substitute variables in values.

    Supports $$ escape sequence for literal $ characters:
    - $$ -> $ (literal dollar sign)
    - $var -> replaced with variable value (if found)
    - $Foo -> $Foo (unchanged if variable not found and strict=False)

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

    # First, handle $$ escapes by temporarily replacing with a placeholder
    working_value = value.replace("$$", _DOLLAR_PLACEHOLDER)

    # Perform regex substitution with single-pass validation
    if strict:
        missing_vars = []

        def _substitute(match: re.Match[str]) -> str:
            var_name = match.group(0)
            if var_name not in lookup:
                missing_vars.append(var_name)
                return var_name
            return str(lookup[var_name])

        result = _DOLLAR_VAR_PATTERN.sub(_substitute, working_value)

        if missing_vars:
            raise ValueError(f"Variable(s) not found in lookup: {', '.join(missing_vars)}")
    else:
        result = _DOLLAR_VAR_PATTERN.sub(lambda match: str(lookup.get(match.group(0), match.group(0))), working_value)

    # Restore escaped dollars
    return result.replace(_DOLLAR_PLACEHOLDER, "$")
