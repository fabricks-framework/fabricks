import ast
from pathlib import Path

_PLAIN_ROOT = Path(__file__).parent.resolve()
_LOCAL_FIXTURES = _PLAIN_ROOT / "fixtures"
_READ_METHODS = {"open", "read_bytes", "read_text"}


def _static_path(node: ast.AST, source: Path, assignments: dict[str, ast.AST]) -> Path | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return Path(node.value)
    if isinstance(node, ast.Name):
        if node.id == "__file__":
            return source
        return _static_path(assignments[node.id], source, assignments) if node.id in assignments else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        left = _static_path(node.left, source, assignments)
        right = _static_path(node.right, source, assignments)
        return left / right if left is not None and right is not None else None
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "Path":
        return _static_path(node.args[0], source, assignments)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "resolve":
        path = _static_path(node.func.value, source, assignments)
        return path.resolve() if path is not None else None
    if isinstance(node, ast.Attribute) and node.attr == "parent":
        path = _static_path(node.value, source, assignments)
        return path.parent if path is not None else None
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute) and node.value.attr == "parents":
        path = _static_path(node.value.value, source, assignments)
        if path is not None and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, int):
            return path.parents[node.slice.value]
    return None


def _external_fixture_reads(source: Path) -> list[Path]:
    tree = ast.parse(source.read_text())
    assignments = {
        target.id: node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    reads = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in _READ_METHODS:
            continue
        path = _static_path(node.func.value, source, assignments)
        if (
            path is not None
            and path.is_relative_to(_PLAIN_ROOT.parent.parent)
            and not path.is_relative_to(_LOCAL_FIXTURES)
        ):
            reads.append(path)
    return reads


def test_plain_tests_only_read_plain_fixtures():
    violations = {
        source.name: [str(path) for path in _external_fixture_reads(source)]
        for source in _PLAIN_ROOT.glob("test_*.py")
        if _external_fixture_reads(source)
    }
    assert violations == {}


def test_static_path_resolves_split_cross_tier_fixture_reference(tmp_path):
    source = tmp_path / "test_example.py"
    expression = ast.parse(
        '_FIXTURE = Path(__file__).resolve().parents[2] / "spark" / "databricks" / "runtime" / "data.sql"'
    ).body[0]
    assignments = {"_FIXTURE": expression.value}

    resolved = _static_path(ast.Name(id="_FIXTURE"), source, assignments)

    assert resolved == tmp_path.resolve().parents[1] / "spark" / "databricks" / "runtime" / "data.sql"
