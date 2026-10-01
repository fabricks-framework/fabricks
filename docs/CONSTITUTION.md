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

Import `databricks.sdk.runtime` (`dbutils`, `spark`) inside the function that
uses it, never at module level: outside a Databricks cluster the import tries
to authenticate against a workspace, so a module-level import makes the module
impossible to import in tests. `tests/unit/plain/test_seams.py` enforces this;
only the notebook entry points in `fabricks/api/notebooks/` are exempt.

Comment only the why, never the what, and only when a future reader would
misread the intent without it. Put longer context somewhere else: the commit
message or PR description for a decision, a note in
[decisions/](./decisions/README.md) for why a design is the way it is,
[DEBUG.md](./DEBUG.md) for a bug signature,
[ARCHITECTURE.md](./ARCHITECTURE.md) for a design constraint.

## 3. Layers

Runtime code imports from `api/`, not framework internals. `metastore/` never
imports `core/`. See [ARCHITECTURE.md](./ARCHITECTURE.md) before crossing a
layer boundary.

`api/` is the stable public surface: do not remove or rename anything from it
(modules, classes, functions, parameters) without explicit user approval —
propose the change and wait. Adding to it is fine.

## 4. Tests

Choose the smallest tier that proves the behavior. See [TEST.md](./TEST.md).
Do not modify or delete an existing test without explicit user approval —
propose the change and wait. CDC tests (`test_cdc.py`, `test_gold_cdc.py`,
`cdc_harness.py`, `test_cdc_harness.py`, `test_cdc_query_generation.py`,
`test_cdc_context.py`) guard core framework behavior; treat changes to them
with extra caution.

Do not modify CDC logic (`framework/fabricks/cdc/`, including its Jinja
templates and the CDC processor code) without explicit user approval —
propose the change and wait. This holds even when a bugfix or failing test
points at CDC code.

## 5. Documentation

Only these six files and the [decisions/](./decisions/README.md) folder are
tracked under `docs/`: this file, [ARCHITECTURE.md](./ARCHITECTURE.md),
[DEBUG.md](./DEBUG.md), [TEST.md](./TEST.md), [WORKFLOW.md](./WORKFLOW.md),
and [SPARK.md](./SPARK.md). Other new documentation paths are ignored. Add concise
content to the matching retained file; do not extend `AGENTS.md` beyond
pointers.
