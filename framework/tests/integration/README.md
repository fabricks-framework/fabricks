# Integration tests

These run against a **real Spark / Databricks** cluster (unlike `tests/unit`, which
mock Spark). Everything under this directory is auto-marked `integration` by
`conftest.py`, so `pytest -m integration` selects exactly this suite.

## Layout

```
integration/
  conftest.py            auto-marks every test here as `integration`
  runtests.py            entrypoint notebook — see "Running" below
  initialize.py          notebook: (re)build landing/raw/out + input & expected tables
  run.py                 notebook: run bronze/silver/gold jobs for data iteration i
  armageddon.py          notebook: full teardown
  add_missing_modules.py notebook: add repo/fabricks/tests to sys.path on the cluster
  helpers/
    const.py             paths & constants: ROOT, PHASES, LANDING, RAW, OUT, STEPS
    seed.py              build test data: git -> landing -> raw -> delta, input tables, expected views
    compare.py           ExpectedSpec + compare_{job,object,cdc}_to_expected assertions
  phases/                the test phases, run in order (see below)
  seed/                  source JSON fixtures (job1..job11 / <topic> / <date>)
  expected/              expected-output SQL views (silver|gold, per scdN)
```

Fixture topic names (`king`, `queen`, `monarch`, ...) map to scenarios in
[`../fixtures/README.md`](../fixtures/README.md).

## Phases

`phases/` holds ordered, **stateful** phases — each builds on the previous, so they
must run in sequence:

| Phase | Purpose |
|---|---|
| `0_armageddon` | teardown + standalone CDC checks |
| `1_schedule` | first scheduled run (iteration 1) — silver/gold/semantic/checks |
| `2_schedule` | second scheduled run (iteration 2) |
| `3_run` | manual `job.run()` path |
| `4_reload` | full-reload behaviour |
| `5_extra` | leftover step-level checks |

The phase list in `runtests.py` is derived from these dir names (`[0-9]_*`), so adding
a phase dir is enough — no list to keep in sync.

## Running

On the cluster, run the **`runtests`** notebook. It:

1. runs `initialize` (build tables for iteration 1),
2. shows a `phases` widget (`*` = all) to pick which phases to run,
3. invokes `pytest phases -k <selected>` and fails the notebook on any test failure.

`initialize` and `run` can also be run standalone via their widgets (`init`, `i`).

## Building the wheel

From the repo root (where `framework/pyproject.toml` lives): `uv build` → `dist/*.whl`.
Cluster/bundle deployment is handled by the `python-test.yml` CI workflow.
