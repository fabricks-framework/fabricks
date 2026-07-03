"""Strip formatting blank lines so ruff can restore a single canonical (dense) form.

Removes blank lines that are pure formatting, then `ruff format` (run right after
in format.sh) re-inserts the PEP8-required blanks around defs/classes and between
methods. Net effect: function bodies stay dense regardless of who wrote them.

Blank lines *inside* multi-line strings (e.g. SQL) are content, not formatting, so
they are protected via tokenize. Databricks notebooks are skipped entirely.
"""

import ast
import sys
import tokenize
import tomllib
from pathlib import Path

# Compound blocks that get a blank line before/after so they stand out in dense code.
_ISOLATE = (ast.For, ast.AsyncFor, ast.While, ast.If, ast.Try)
_CLAUSES = ("else", "elif", "except", "finally", "case")
# Clauses that DO get a blank before them when a nested block is the suite's tail (e.g. try body).
_TAIL_CLAUSES = ("except", "finally")
_STRING_TOKENS = {tokenize.STRING} | {
    getattr(tokenize, n) for n in ("FSTRING_START", "FSTRING_MIDDLE", "FSTRING_END") if hasattr(tokenize, n)
}


def _protected_lines(src: str) -> set[int] | None:
    """1-based line numbers inside multi-line string literals, or None if un-tokenizable."""
    protected: set[int] = set()

    try:
        tokens = tokenize.generate_tokens(iter(src.splitlines(keepends=True)).__next__)

        for tok in tokens:
            if tok.type in _STRING_TOKENS and tok.end[0] > tok.start[0]:
                protected.update(range(tok.start[0], tok.end[0] + 1))

    except (tokenize.TokenError, IndentationError, SyntaxError):
        # Un-tokenizable (mid-edit) -> bail rather than risk corrupting the file.
        return None

    return protected


def standardize(src: str) -> str:
    if src.startswith("# Databricks notebook source"):
        return src

    protected = _protected_lines(src)

    if protected is None:
        return src

    lines = src.splitlines(keepends=True)
    kept = [ln for i, ln in enumerate(lines, start=1) if ln.strip() or i in protected]

    return _isolate_blocks("".join(kept))


def _suites(node: ast.AST):
    for attr in ("body", "orelse", "finalbody"):
        if suite := getattr(node, attr, None):
            yield suite

    for handler in getattr(node, "handlers", []):
        if handler.body:
            yield handler.body


def _is_stub(stmt: ast.AST) -> bool:
    return (
        isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
        and len(stmt.body) == 1
        and isinstance(stmt.body[0], ast.Expr)
        and isinstance(stmt.body[0].value, ast.Constant)
        and stmt.body[0].value.value is ...
    )


def _is_cache_call(stmt: ast.AST) -> bool:
    """`df.cache()` — mutates Spark plan state, so it's isolated to stay visible."""
    return (
        isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Call)
        and isinstance(stmt.value.func, ast.Attribute)
        and stmt.value.func.attr == "cache"
    )


def _mark(stmts: list, before: set[int], after: set[int], after_clause: set[int]) -> None:
    """Flag blank lines to insert before/after statements that should stand out in dense code."""
    last = len(stmts) - 1

    for i, stmt in enumerate(stmts):
        if isinstance(stmt, _ISOLATE):
            if i > 0:
                before.add(stmt.lineno)

            if i < last:
                after.add(stmt.end_lineno)
            else:
                # Last in suite: separate it from an enclosing except/finally (e.g. try body).
                after_clause.add(stmt.end_lineno)

        if _is_stub(stmt) and i < last:
            after.add(stmt.end_lineno)

        if _is_cache_call(stmt):
            if i > 0:
                before.add(stmt.lineno)

            if i < last:
                after.add(stmt.end_lineno)

        if isinstance(stmt, ast.Try) and stmt.finalbody:
            # Isolate `finally`: blank before it so cleanup stands out in dense code.
            pre = stmt.orelse or (stmt.handlers[-1].body if stmt.handlers else stmt.body)
            after_clause.add(pre[-1].end_lineno)

        # Isolate a `return`: blank after (so guard clauses separate; skipped before else/EOF in
        # rebuild) and blank before whenever it has a sibling above.
        if isinstance(stmt, ast.Return):
            after.add(stmt.end_lineno)

            if i > 1:
                before.add(stmt.lineno)

        for suite in _suites(stmt):
            _mark(suite, before, after, after_clause)


def _isolate_blocks(src: str) -> str:
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return src

    before: set[int] = set()
    after: set[int] = set()
    after_clause: set[int] = set()
    _mark(tree.body, before, after, after_clause)

    if not before and not after and not after_clause:
        return src

    lines = src.splitlines(keepends=True)
    out: list[str] = []

    for idx, line in enumerate(lines, start=1):
        if idx in before:
            # Insert above any comment lines that hug the block, keeping them attached.
            j = len(out)

            while j > 0 and out[j - 1].lstrip().startswith("#"):
                j -= 1

            if j > 0 and out[j - 1].strip():
                out.insert(j, "\n")

        out.append(line)
        nxt = lines[idx] if idx < len(lines) else ""

        # No blank right before a continuation clause (else/elif/except/finally/case) or at EOF.
        if idx in after and nxt and not _starts_clause(nxt):
            out.append("\n")
        # ...except a nested block at the suite's tail does get split from except/finally.
        elif idx in after_clause and _starts_clause(nxt, _TAIL_CLAUSES):
            out.append("\n")

    return "".join(out)


def _starts_clause(line: str, clauses: tuple[str, ...] = _CLAUSES) -> bool:
    s = line.lstrip()
    return any(s == kw or s.startswith(kw + " ") or s.startswith(kw + ":") for kw in clauses)


def _excludes() -> list[str]:
    """Folder names to skip, from [tool.standardize].exclude in pyproject.toml."""
    pyproject = Path(__file__).with_name("pyproject.toml")

    if not pyproject.exists():
        return []

    cfg = tomllib.loads(pyproject.read_text())

    return cfg.get("tool", {}).get("standardize", {}).get("exclude", [])


def main(targets: list[str]) -> None:
    exclude = set(_excludes())

    for target in targets:
        root = Path(target)
        files = root.rglob("*.py") if root.is_dir() else [root]

        for path in files:
            if exclude.intersection(path.parts):
                continue

            src = path.read_text(encoding="utf-8")
            out = standardize(src)

            if out != src:
                path.write_text(out, encoding="utf-8")
                print(f"standardized {path} (-{src.count(chr(10)) - out.count(chr(10))} blank lines)")


if __name__ == "__main__":
    main(sys.argv[1:] or ["."])
