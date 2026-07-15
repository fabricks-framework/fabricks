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
    const.py             paths & constants: ROOT, RAW_DATA, PHASES, LANDING, RAW, OUT, STEPS
    generate_data.py     seed/ -> raw/ : apply every transform once (pure stdlib, no Spark)
    seed.py              load raw/ into landing -> raw storage -> delta, input tables, expected views
    compare.py           ExpectedSpec + compare_{job,object,cdc}_to_expected assertions
  phases/                the test phases, run in order (see below)
  seed/                  original source JSON fixtures (job1..job11 / <topic> / <date>) — git origin
  raw/                   transformed fixtures generated from seed/ — what the tests actually load
  expected/              expected-output SQL views (silver|gold, per scdN)
```

Fixture topic names (`king`, `queen`, `monarch`, ...) map to scenarios in
[`../fixtures/README.md`](../fixtures/README.md).

## Fixture data: `seed/` → `raw/`

`seed/` holds the **original, git-versioned** JSON (with `BEL_*` change-tracking columns
and one folder per source). `helpers/generate_data.py` transforms it **once** into `raw/`
(named to mirror the bronze `raw/<topic>` uris in the yml configs), and everything at
runtime (`seed.py`: landing/raw storage/delta + input tables) reads `raw/`
**as-is — no further transformation**.

What `generate_data.py` bakes into `raw/`:

- adds `__operation` (from `BEL_*` / deletelog) and `__timestamp` (from the date folder),
  then drops the `BEL_*` columns;
- derives `monarch`/`regent` by aliasing `king`/`queen` (regent merges its deletelog into
  one folder; monarch keeps a separate `__deletelog` folder);
- builds `royal` as a `reload` snapshot: the cumulative **current** state (latest per id,
  deletes applied by omission), one load per job sharing the batch's folder timestamp — so
  it lines up with the other topics' current rows;
- passes non-topic folders (`too_many_columns`, `prince__deletelog`, ...) through unchanged.

Regenerate after editing `seed/`: `python helpers/generate_data.py` (commit `raw/`).
The only value `seed.py` still synthesises is `decimalField` (a decimal→double
type-conversion fixture asserted by `test_silver`), which is pipeline artifact, not data.

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
