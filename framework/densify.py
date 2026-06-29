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
_ISOLATE = (ast.For, ast.AsyncFor, ast.While, ast.If)
# Blocks that, when sitting directly above a `return`, get a blank line between them.
_BLOCK = (ast.For, ast.AsyncFor, ast.While, ast.If, ast.With, ast.AsyncWith, ast.Try)
_CLAUSES = ("else", "elif", "except", "finally", "case")
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


def densify(src: str) -> str:
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


def _mark(stmts: list, before: set[int], after: set[int]) -> None:
    """Flag a blank line before/after each isolated block that has a sibling on that side."""
    last = len(stmts) - 1

    for i, stmt in enumerate(stmts):
        # Only isolate an `if` when it has elif/else; a plain guard `if` stays inline.
        plain_if = isinstance(stmt, ast.If) and not stmt.orelse
        if isinstance(stmt, _ISOLATE) and not plain_if:
            if i > 0:
                before.add(stmt.lineno)
            if i < last:
                after.add(stmt.end_lineno)
        # Blank line after a `return` so guard clauses separate (skipped before else/EOF in rebuild).
        # And a blank before it when it directly follows a block, separating the result from the block.
        if isinstance(stmt, ast.Return):
            after.add(stmt.end_lineno)
            if i > 0 and isinstance(stmts[i - 1], _BLOCK):
                before.add(stmt.lineno)

        for suite in _suites(stmt):
            _mark(suite, before, after)


def _isolate_blocks(src: str) -> str:
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return src

    before: set[int] = set()
    after: set[int] = set()
    _mark(tree.body, before, after)
    if not before and not after:
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
        # No blank right before a continuation clause (else/elif/except/finally/case) or at EOF.
        if idx in after and idx < len(lines) and not _starts_clause(lines[idx]):
            out.append("\n")

    return "".join(out)


def _starts_clause(line: str) -> bool:
    s = line.lstrip()
    return any(s == kw or s.startswith(kw + " ") or s.startswith(kw + ":") for kw in _CLAUSES)


def _excludes() -> list[str]:
    """Folder names to skip, from [tool.densify].exclude in pyproject.toml."""
    pyproject = Path(__file__).with_name("pyproject.toml")
    if not pyproject.exists():
        return []

    cfg = tomllib.loads(pyproject.read_text())
    return cfg.get("tool", {}).get("densify", {}).get("exclude", [])


def main(targets: list[str]) -> None:
    exclude = set(_excludes())

    for target in targets:
        root = Path(target)
        files = root.rglob("*.py") if root.is_dir() else [root]

        for path in files:
            if exclude.intersection(path.parts):
                continue
            src = path.read_text()
            out = densify(src)
            if out != src:
                path.write_text(out)
                print(f"densified {path} (-{src.count(chr(10)) - out.count(chr(10))} blank lines)")


if __name__ == "__main__":
    main(sys.argv[1:] or ["."])
