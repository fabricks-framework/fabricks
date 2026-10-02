"""Fail on comment blocks longer than the limit in the given files: a longer explanation belongs in the commit message,
docs/decisions/ or docs/DEBUG.md (see the `comment-fabricks` skill).

Usage: python scripts/check_comment_size.py FILE...  (the pre-commit hook passes the staged files)
"""

import io
from pathlib import Path
import sys
import tokenize

MAX_BLOCK_LINES = 3
NOTEBOOK_MARKERS = ("# MAGIC", "# COMMAND", "# DBTITLE", "# Databricks notebook source")
SKIPPED = "tests/unit/plain/fixtures/"  # parser input, not our comments


def comment_lines(path: Path) -> list[int]:
    text = path.read_text()
    if path.suffix == ".jinja":
        return [n for n, line in enumerate(text.splitlines(), 1) if line.lstrip().startswith("--")]
    return [
        token.start[0]
        for token in tokenize.generate_tokens(io.StringIO(text).readline)
        if token.type == tokenize.COMMENT
        and token.line.lstrip().startswith("#")
        and not token.string.startswith(NOTEBOOK_MARKERS)
    ]


def long_blocks(path: Path) -> list[tuple[int, int]]:
    """(first line, length) of every run of consecutive whole-line comments longer than the limit."""
    blocks: list[tuple[int, int]] = []
    run: list[int] = []
    for line in [*comment_lines(path), None]:
        if run and (line is None or line != run[-1] + 1):
            if len(run) > MAX_BLOCK_LINES:
                blocks.append((run[0], len(run)))
            run = []
        if line is not None:
            run.append(line)
    return blocks


def main(files: list[str]) -> int:
    failed = False
    for name in files:
        path = Path(name)
        if path.suffix not in {".py", ".jinja"} or SKIPPED in path.resolve().as_posix() or not path.exists():
            continue
        for start, length in long_blocks(path):
            failed = True
            print(f"{name}:{start}: comment block of {length} lines (max {MAX_BLOCK_LINES})")
    if failed:
        print("Shorten the comment, or move the explanation to the commit message, docs/decisions/ or docs/DEBUG.md.")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
