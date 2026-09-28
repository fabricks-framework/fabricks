import ast
import json
import re

from fabricks.utils.sqlglot import get_tables

_MAGIC = re.compile(r"^\s*%(\w+)")
_IDENTIFIER_TEMPLATE = re.compile(r"(?i)identifier\(\{")


class _UnresolvableError(Exception):
    pass


def get_code_cells(ipynb_text: str) -> list[str]:
    """Source of every code cell in a Jupyter notebook, in cell order."""
    notebook = json.loads(ipynb_text)
    cells = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        cells.append("".join(source) if isinstance(source, list) else source)
    return cells


def _base_name(node: ast.expr) -> str | None:
    """Walk a (possibly chained) call/attribute expression down to its root Name, e.g.
    `spark.read.format(...).table` -> "spark"."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _base_name(node.value)
    if isinstance(node, ast.Call):
        return _base_name(node.func)
    return None


def _resolve_str(node: ast.expr, constants: dict[str, str]) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    raise _UnresolvableError


def _substitute_identifier_placeholders(sql: str, keywords: list[ast.keyword]) -> tuple[str, bool]:
    """Spark's `identifier({param})` templating is a value placeholder sqlglot can't resolve on
    its own -- substitute in literal keyword values so the referenced table becomes visible.
    Returns (sql, unresolved) where unresolved is True if any keyword wasn't a literal string."""
    unresolved = False
    for keyword in keywords:
        if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str) and keyword.arg:
            pattern = r"(?i)identifier\(\{\s*" + re.escape(keyword.arg) + r"\s*\}\)"
            sql = re.sub(pattern, keyword.value.value, sql)
        else:
            unresolved = True
    return sql, unresolved


def _collect_constants(tree: ast.Module, constants: dict[str, str]) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            constants[node.targets[0].id] = node.value.value


# (base, attr) of a recognized table-read call -> index of its table/SQL argument.
_TABLE_CALLS = {("spark", "sql"): 0, ("spark", "table"): 0, ("DeltaTable", "forName"): 1}


def _collect_sql(tree: ast.Module, constants: dict[str, str], sql_snippets: list[str]) -> bool:
    """Appends every statically-resolvable table read to sql_snippets. Returns True if an
    unresolvable table reference was encountered (signals the caller to fall back)."""
    unresolved = False

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue

        attr = node.func.attr
        base = _base_name(node.func.value)
        if base is None:
            continue
        index = _TABLE_CALLS.get((base, attr))
        if index is None or len(node.args) <= index:
            continue

        try:
            value = _resolve_str(node.args[index], constants)
        except _UnresolvableError:
            unresolved = True
            continue

        if attr == "sql":
            if _IDENTIFIER_TEMPLATE.search(value):
                value, unresolved_placeholder = _substitute_identifier_placeholders(value, node.keywords)
                unresolved = unresolved or unresolved_placeholder
            sql_snippets.append(value)
        else:
            sql_snippets.append(f"select * from {value}")

    return unresolved


def get_notebook_dependencies(ipynb_text: str, allowed_databases: list[str] | None = None) -> list[str] | None:
    """Statically parses a Jupyter notebook's cells for spark.sql/spark.table/DeltaTable.forName
    table reads. Returns None when a construct couldn't be resolved (dynamic SQL, an
    unresolvable variable, %run, or no dependency found at all) -- the caller should fall back
    to execution-based dependency resolution in that case.

    # ponytail: None conflates "confidently zero deps" with "couldn't resolve, don't trust this"
    # -- fine while the only caller (Gold._get_notebook_dependencies) treats both the same way.
    # Split into a richer result type if a second caller needs to tell them apart.
    """
    constants: dict[str, str] = {}
    sql_snippets: list[str] = []
    unresolved = False

    for cell in get_code_cells(ipynb_text):
        magic = _MAGIC.match(cell)
        if magic:
            name = magic.group(1)
            if name == "sql":
                _, _, sql = cell.partition("\n")
                sql_snippets.append(sql)
            elif name == "run":
                unresolved = True
            continue

        try:
            tree = ast.parse(cell)
        except SyntaxError:
            continue

        _collect_constants(tree, constants)
        if _collect_sql(tree, constants, sql_snippets):
            unresolved = True

    dependencies = get_tables(";\n".join(sql_snippets), allowed_databases=allowed_databases) if sql_snippets else []

    if unresolved or not dependencies:
        return None
    return dependencies
