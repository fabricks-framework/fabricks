# Constitution

Rules for `framework/`. Direct user instruction takes precedence over these
rules; [AGENTS.md](../AGENTS.md) is the entry point.

## 1. Databricks LTS

Keep dependency minimums at or below the target Databricks LTS runtime. Check
the runtime package list before raising a minimum version.

## 2. Coding Style

Use the repository formatters and linters rather than hand formatting:
`just format` and `just lint`, run from `framework/`. Non-test code needs
explicit types. Prefer `pathlib` and the standard library over custom path
handling. Run `just setup-hooks` once per checkout to enable the pre-commit
format+lint hook. Keep each file to one purpose and a manageable size; split
a file that has grown multiple responsibilities instead of extending it. See
the `coding-guidelines-python` skill for typing, Pyright, dataclasses,
enums, and other Python-specific conventions, and the `code-quality` skill
for ruff/mypy config and fail-loud/determinism anti-patterns.

## 3. Layers

Runtime code imports from `api/`, not framework internals. `metastore/` never
imports `core/`. See [ARCHITECTURE.md](./ARCHITECTURE.md) before crossing a
layer boundary.

## 4. Tests

Choose the smallest tier that proves the behavior. See [TEST.md](./TEST.md).
Do not modify or delete an existing test without explicit user approval —
propose the change and wait. CDC tests (`test_cdc.py`, `test_gold_cdc.py`,
`cdc_harness.py`, `test_cdc_harness.py`, `test_cdc_query_generation.py`,
`test_cdc_context.py`) guard core framework behavior; treat changes to them
with extra caution.

## 5. Documentation

Only these six files are tracked under `docs/`: this file,
[ARCHITECTURE.md](./ARCHITECTURE.md), [DEBUG.md](./DEBUG.md),
[TEST.md](./TEST.md), [WORKFLOW.md](./WORKFLOW.md), and
[SPARK.md](./SPARK.md). New documentation paths are ignored. Add concise
content to the matching retained file; do not extend `AGENTS.md` beyond
pointers.
