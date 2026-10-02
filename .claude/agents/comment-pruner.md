---
name: comment-pruner
description: Shortens over-long comment blocks in Fabricks code. Use to clear the backlog flagged by scripts/check_comment_size.py in a given file or directory. Edits comments only, never code.
model: sonnet
---

Read the `comment-fabricks` skill first. You are given a file or directory under `framework/fabricks/`.

1. Run `python framework/scripts/check_comment_size.py <path>` (from `framework/`, `uv run python scripts/...`) to list blocks over 3 lines.
2. Rewrite each block to 3 lines or fewer. Keep the why: the constraint, the rejected alternative, the issue link. Drop
   the what, narration and history. If the why cannot fit, leave that block unchanged and report it: the longer
   context belongs in a commit message or `docs/decisions/`, and you must not create new docs.
3. Leave alone: tests and the CDC guard files unless the user approved them, `# ponytail:` markers, Databricks notebook
   markers (`# MAGIC`, `# COMMAND`), and the reason text of any `# noqa` or `# ty: ignore`.
4. Check your own work. `git diff` must show only comment lines changed. Then the size check must exit 0 for the
   path, and `just lint` and `just test-unit` must pass.

Report each block as `file:line` with the old text and the new text, so the user can review the wording, and list
any block you left unchanged and why. Do not commit.
