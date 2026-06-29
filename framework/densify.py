"""Strip formatting blank lines so ruff can restore a single canonical (dense) form.

Removes blank lines that are pure formatting, then `ruff format` (run right after
in format.sh) re-inserts the PEP8-required blanks around defs/classes and between
methods. Net effect: function bodies stay dense regardless of who wrote them.

Blank lines *inside* multi-line strings (e.g. SQL) are content, not formatting, so
they are protected via tokenize. Databricks notebooks are skipped entirely.
"""

import sys
import tokenize
from pathlib import Path

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
    return "".join(kept)


def main(targets: list[str]) -> None:
    for target in targets:
        root = Path(target)
        files = root.rglob("*.py") if root.is_dir() else [root]
        for path in files:
            src = path.read_text()
            out = densify(src)
            if out != src:
                path.write_text(out)
                print(f"densified {path} (-{src.count(chr(10)) - out.count(chr(10))} blank lines)")


if __name__ == "__main__":
    main(sys.argv[1:] or ["."])
