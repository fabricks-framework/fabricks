"""Seam guards: production modules must stay importable without a Databricks runtime."""

import ast
import importlib.util
from pathlib import Path

_NOTEBOOK_ENTRY_POINTS = "api/notebooks"  # Databricks notebook sources import dbutils at module level by design


def _fabricks_root() -> Path:
    spec = importlib.util.find_spec("fabricks")
    assert spec
    assert spec.submodule_search_locations
    return Path(spec.submodule_search_locations[0])


def _module_level_nodes(node: ast.AST):
    """Yield statements that execute at import time (skip function bodies)."""
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            yield from _module_level_nodes(child)


def _imports_sdk_runtime(node: ast.AST) -> bool:
    if isinstance(node, ast.ImportFrom):
        return (node.module or "").startswith("databricks.sdk.runtime")
    if isinstance(node, ast.Import):
        return any(a.name.startswith("databricks.sdk.runtime") for a in node.names)
    return False


def test_no_module_level_databricks_sdk_runtime_import():
    root = _fabricks_root()
    offenders = []
    for path in root.rglob("*.py"):
        if _NOTEBOOK_ENTRY_POINTS in path.as_posix():
            continue
        tree = ast.parse(path.read_text())
        for node in _module_level_nodes(tree):
            if _imports_sdk_runtime(node):
                offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert not offenders, f"module-level databricks.sdk.runtime imports (move inside the function): {offenders}"
