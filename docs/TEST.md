# Testing

Two suites under `framework/tests/`, split by whether they need a real
Spark/Delta session. Both are plain `pytest`, distinguished by marker
(`unit` / `integration`, declared in `pyproject.toml`
`[tool.pytest.ini_options]`); each suite's `conftest.py` auto-applies its
marker to every test under it, so you don't need to tag tests by hand.

## Unit tests — `tests/unit/`

Run locally, no Databricks cluster needed. `tests/unit/conftest.py` mocks
`fabricks.utils.spark` / `fabricks.context` at module level before any
`fabricks` import happens, so code under test never touches a real
`SparkSession`. Use these for logic that doesn't need Spark to prove itself:
path handling (`test_git_path.py`), YAML/config parsing
(`test_read_yaml.py`), variable substitution (`test_variable_substitution.py`).

```
uv run pytest tests/unit
```

## Integration tests — `tests/databricks/`

Exercise real jobs end-to-end against a real Databricks cluster
(`databricks-connect`) and a real fixture runtime checked into this repo at
`tests/databricks/runtime/` (its own `bronze/`, `silver/`, `gold/`, parsers,
UDFs, extenders, masks, views, schedules, and
`conf.uc.fabricks.yml` / `conf.5589296195699698.yml`). They are not
self-contained pytest — they run as Databricks notebooks
(`# Databricks notebook source` header, driven by `dbutils.widgets`),
because they need a live cluster with the runtime deployed:

- `tests/databricks/runtests.py` — the entry notebook. Widgets control
  `initialize` / `armageddon` (full data reset) / `reset` / `fix_notebooks`,
  and which of `job1`..`job5` to run. It resolves to `pytest -k <selection>`
  under `jobs/`.
- `tests/databricks/init.sh` — cluster init script; sets
  `FABRICKS_RUNTIME` / `FABRICKS_NOTEBOOKS` / `FABRICKS_CONFIG` env vars and
  pip-installs the test/runtime dependencies onto the cluster.
- `tests/databricks/jobs/job1/…job5/` — the actual test modules, ordered
  with `pytest.mark.order(...)` (via `pytest-order`) because later jobs
  depend on tables earlier jobs produced (schedules, CDC reload, invoke,
  dependency resolution, checks, etc. each get their own `test_*.py`).
- `tests/expected/{silver,gold}/{scd0,scd1,scd2}/job*.sql` — golden SQL
  snapshots, a top-level sibling of `tests/databricks/`/`tests/unit/`
  (shared, not owned by either suite). `compare.py` builds the job's
  generated SQL and diffs it against these; a deliberate SQL-generation
  change means regenerating the matching snapshot, not hand-editing it to
  make the diff pass.
- `tests/databricks/phases/0_armageddon/` .. `phases/5_extra/` — the same
  jobs grouped into ordered phases (full reset → first schedule → second
  schedule → a plain run → CDC reload → step-level extras) mirroring what a
  real deployment does over its lifetime.

There is no local way to run these — they need the fixture runtime deployed
to an actual Databricks workspace/cluster with `init.sh` applied. Treat a
change to `core/`, `cdc/`, or `metastore/` that only unit tests cover as
under-tested; if the logic depends on real Spark/Delta/Unity Catalog
behavior, it needs an integration job (or a new one) under `jobs/`, not just
mocked unit coverage.

## Rule of thumb

Changed SQL generation → check whether an `expected/**/job*.sql` snapshot
needs regenerating. Otherwise, pick unit vs. integration by whether the
logic needs a real Spark/Delta session, per the two sections above — and
expect to confirm an integration change on a cluster, not just by reading
the diff.

See [CONSTITUTION.md § V](./CONSTITUTION.md) for when a change requires a
test at all, and [ARCHITECTURE.md](./ARCHITECTURE.md) for how the pieces
under test fit together.
