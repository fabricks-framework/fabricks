"""The plain tier must not depend on the Spark tiers' test code, and the shared support package must not either.

tests/support/ holds the pure helpers both sides use. Anything under tests/spark/ may need a session, the Apache
conftest, or a Databricks notebook context.
"""

import ast
from pathlib import Path

_TESTS_ROOT = Path(__file__).resolve().parents[2]
_PLAIN = _TESTS_ROOT / "unit" / "plain"
_SUPPORT = _TESTS_ROOT / "support"
# Notebook-safe by design (Databricks imports it as a sibling), so it cannot live in tests/support/.
_ALLOWED = {("test_generate_local_fixtures.py", "tests.spark.databricks.fixtures")}


def _spark_tier_imports(source: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            modules |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return {m for m in modules if m == "tests.spark" or m.startswith("tests.spark.")}


def test_plain_tests_do_not_import_spark_tier_code():
    violations = {
        source.name: sorted(m for m in _spark_tier_imports(source) if (source.name, m) not in _ALLOWED)
        for source in _PLAIN.glob("test_*.py")
    }
    assert {name: mods for name, mods in violations.items() if mods} == {}


def test_support_package_does_not_import_spark_tier_code():
    violations = {source.name: sorted(_spark_tier_imports(source)) for source in _SUPPORT.glob("*.py")}
    assert {name: mods for name, mods in violations.items() if mods} == {}


def test_the_guard_sees_spark_tier_imports(tmp_path):
    source = tmp_path / "mod.py"
    source.write_text(
        "import tests.spark.apache.x\nfrom tests.spark.expected import compare\nfrom tests.support import y\n"
    )

    assert _spark_tier_imports(source) == {"tests.spark.apache.x", "tests.spark.expected"}
