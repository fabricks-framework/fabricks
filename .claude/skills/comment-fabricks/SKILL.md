---
name: comment-fabricks
description: Decide whether a comment belongs in Fabricks code, and write or audit comments and docstrings (Python, Jinja/SQL templates, notebooks, YAML, tests). Use when writing code that might need a comment, when asked to review or clean up comments or docstrings, and before finishing any change that added or touched comments.
---

# Comments in Fabricks

Adapted from bmsuisse's `code-comments` skill, narrowed to this repo. The rule is in `docs/CONSTITUTION.md` § 2 (it wins on conflict): **comment only the why, never the what, and only when a future reader would misread the intent without it.**

**Default to fewer comments.** A missing comment costs a reader seconds; a useless one taxes every reader forever. A file with clear names and no comments beats the same file with five comments restating them. Never comment to look thorough.

## The one test

If you deleted this comment, would a competent maintainer be missing something they cannot get from the code, a better name or a type? No: delete it, or rename or restructure so it was never needed. Yes: keep it, and make it say *why*.

## Where context goes instead

A comment is the last resort. Put longer context in the place built for it, so the code stays quiet:

| What you want to say | Put it in |
|---|---|
| Why this change was made, what it fixed | commit message / PR description |
| Why a design is the way it is, what was rejected | `docs/decisions/NNNN-title.md` |
| A bug signature someone will hit again | `docs/DEBUG.md` |
| A structural constraint (layers, imports) | `docs/ARCHITECTURE.md` |
| A Spark plan or CDC performance finding | `docs/SPARK.md` |
| What a test proves and which issue it reproduces | the test's module docstring (issue link plus the invariant) |

Link to the note from the code only when a reader would otherwise "fix" something deliberate: `# See docs/decisions/0003-...`.

## What earns a comment here

- A workaround tied to a concrete constraint: a Databricks, Delta, Spark Connect or Py4J quirk, a runtime version limit, an ordering requirement. Name the constraint.
- A rejected obvious alternative, so nobody helpfully redoes it.
- A business or external rule not derivable from the function (a vendor format, a spec value).
- A deliberate shortcut: keep `# ponytail: <what was skipped and the cost>` markers; they feed the `ponytail-debt` ledger. Don't strip or reword them.
- A public contract: a docstring on exported API (`fabricks/api/`) stating inputs, outputs, side effects and invariants. Types already say the types.

## Strip on sight

- Restating the code (`# loop over jobs`), or the type (`# list of steps` above `steps: list[Step]`).
- Commented-out code (git has it), change-log or narration (`# fixed for #123 in 2025`; that is the commit message), apologies, venting, jokes standing in for a fix.
- A TODO without what and why. Use `# TODO(#<issue>): <what is missing and why not now>` or no TODO.
- Section banners and decorative separators in code files.

## Keep untouched (they look like comments, but are not)

- Databricks notebook markers: `# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC ...`. Deleting them breaks the notebook (`fabricks/api/notebooks/`, `tests/spark/databricks/`).
- Tool directives: `# noqa`, `# ty: ignore[...]`, `# type:`, `# pragma`. Add a short reason only when the suppression is not self-evident.
- Fixture notebooks under `tests/unit/plain/fixtures/notebooks/` are raw input for the parser under test; their comments are test data.

## Per file kind

- **Python:** docstrings for public API and for tests' module intent; inline `#` only for the why. Line length is 119 (ruff), so don't wrap a one-line why into a paragraph. If a comment needs a paragraph, it belongs in `docs/decisions/` or the commit.
- **Jinja / SQL templates** (`fabricks/cdc/templates/`): `--` comments are emitted into the generated SQL; `{# ... #}` are not. Use `{# #}` for notes to maintainers, and never put a comment in a template that a SQL-shape test would match against.
- **YAML runtime and job config:** comment only a non-obvious option value or an unusual override, with the reason.
- **Tests:** the module docstring links the issue and states the invariant; a name plus assertion message beats a comment. A comment explaining why a fake or mock is set a certain way is wanted (see `testing-fabricks`).

## Writing mode

1. Try to make the comment unnecessary: rename, extract a named helper, add a type.
2. Still needed? Write the constraint or the rejected alternative, not the mechanics.
3. Longer than two lines? Move it per the table above and link it.
4. Never vent, apologize or joke instead of fixing. If something is bad, fix it, or name the real constraint stopping you.

## Review mode

For a file or diff: `git diff -U0 <base> | grep -nE '^\+\s*(#|--|\{#|""")'` lists added comments, or read the file directly. Judge each against the one test and report one line per action:

```
## Comment review
### Delete      - file.py:42 restates the assignment
### Rewrite     - file.py:110 says what the regex does; say why this pattern
### Move        - file.py:8 three-line history; belongs in the commit / a decision note
### Add         - api.py:30 exported function, non-obvious contract, needs a docstring
### Keep        - db.py:77 rejected alternative, exactly right
```

If the comments are already clean, say so; don't invent findings. Deleting or rewording comments inside a test file is a change to an existing test: follow `docs/CONSTITUTION.md` § 4 (propose, wait for approval).

## Tone

Comments are read by colleagues and whoever is on call at 3am. No sarcasm or blame aimed at a person. Name the actual issue instead.
