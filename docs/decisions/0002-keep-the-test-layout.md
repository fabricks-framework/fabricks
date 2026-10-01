# 0002: Keep the current test layout

## Context

The tiers live under two parents, `tests/unit/{plain,config}` and
`tests/spark/{apache,databricks}`, and the runtime fixtures sit in
`tests/spark/runtime/` (used by config and apache) next to a separate
`tests/spark/databricks/runtime/` (used only by Databricks). A flat
`tests/{plain,config,apache,databricks}/` plus `tests/runtime/` would match the
tier names better.

Measured cost of that rename: about 60 files reference the paths (15 for
`tests/unit/plain`, 12 for `unit/config`, 20 each for `spark/apache` and
`spark/databricks`, 10 for `spark/runtime`), including `databricks.yml` (bundle
sync and notebook paths), `pyproject.toml`, `ruff.toml`, `.vscode/settings.json`,
the CI workflow, the justfile, `tier_policy.py` and four docs. The Databricks
tier cannot be run locally, so a path mistake there would only show up on a
workspace.

## Decision

Do not rename. Tier selection is by path through one table (`_PATH_TIERS` in
`tests/tier_policy.py`), the tiers are documented in `docs/TEST.md`, and the
layout is no longer misleading: `unit/` holds the two local-only tiers (see
0001), `spark/` holds the two that need a Spark session.

`tests/spark/runtime/` stays shared by config and apache, because its YAML is a
runtime config, not Spark code. `tests/spark/databricks/runtime/` stays apart
because it is bundle-synced to a workspace and must be notebook-safe.

## Consequences

- Revisit only if a fifth tier appears or the Databricks tier can be run from
  CI, which removes the unverifiable-path risk.
- Pure helpers shared across tiers go in `tests/support/`, never in a tier
  folder (enforced for plain by `test_tier_boundary.py`).
- File names that describe a symptom (`test_incremental_filter_full_scan.py`)
  are not worth a history-rewriting rename on their own.
