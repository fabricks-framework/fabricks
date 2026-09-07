# Architecture

Agent-facing map of the `fabricks` package (`framework/fabricks/`). Read this
before navigating the codebase; it says what lives where and how the pieces
call into each other. It does not repeat the end-user pipeline concepts
(steps, CDC, checks) — those belong in the package's own `README.md`.

## What Fabricks is

A Python framework that runs on Databricks (PySpark + Delta) and turns a
declarative runtime — YAML job configs plus SQL transformation files — into
a scheduled, dependency-ordered set of table/view builds, with built-in
change-data-capture (SCD0/1/2) and data-quality checks. Business logic is
SQL; Fabricks provides the orchestration, table lifecycle, and CDC machinery
around it.

## Package layout (`framework/fabricks/`)

| Package | Responsibility |
|---|---|
| `api/` | **Public surface.** What a runtime's notebooks and extension code (parsers, UDFs, extenders) import. Thin re-exports over `core`/`context`/`deploy`; keep it that way — new capability goes in `core`/`context` and gets exposed here only if it's meant to be user-facing. `api/notebooks/` holds the notebook templates (`initialize`, `run`, `process`, `terminate`, `standalone`, `cluster`) that a deployed runtime actually schedules in Databricks. |
| `context/` | Process-wide state, built once at import time from the runtime's YAML config: mode flags and paths (`config.py`), the parsed runtime config and step registries (`runtime.py`), the Spark session (`spark_session.py`), secrets (`secret.py`), logging (`log.py`). Because this is import-time singleton state, changing it means changing what every job sees for the rest of the process. |
| `core/` | The engine. `core/steps` (`BaseStep`, `get_step`) models a pipeline stage (bronze/silver/gold-family); `core/jobs` models one topic+item unit of work and its lifecycle (configure → generate SQL → check → process/invoke) — `bronze.py`/`silver.py`/`gold.py` per step family, shared lifecycle logic under `core/jobs/base/`; `core/dags` builds and executes the dependency DAG across jobs for a schedule; `core/schedules` ties a named schedule to the set of jobs/views it must (re)build; `core/parsers` loads user-supplied parser Python from the runtime path; `core/extenders.py` and `core/masks.py` (plain modules, not packages) apply user-supplied extenders and column masking. |
| `cdc/` | Change-data-capture engines, independent of Spark orchestration concerns: `scd0.py`/`nocdc.py` (no history), `scd1.py` (current-state upsert, `__is_current`/`__is_deleted`), `scd2.py` (validity windows, `__valid_from`/`__valid_to`), all built on shared merge-SQL generation/execution under `cdc/base/`. `cdc/templates/` holds the Jinja/SQL templates the generator fills in. |
| `metastore/` | Delta table/view/database primitives (`table.py`, `view.py`, `database.py`, `dbobject.py`) — creation, schema evolution, drop/recreate, options (partitioning, clustering, properties). `core/jobs` and `cdc` call down into this to actually create/alter storage; it has no knowledge of jobs, steps, or schedules. |
| `models/` | Pydantic models for everything parsed from YAML or passed between layers: `runtime/` (the full runtime config schema), `config.py`/`config/` (project-level config), `job.py`/`job_schema.py`, `step.py`, `schedule.py`, `dependency.py`, `table.py`, `path.py`, `common.py`. Treat these as the schema contract — a new YAML field starts here. |
| `deploy/` | One-shot setup/sync actions invoked when a runtime is (re)deployed: `runtime.py`, `tables.py`, `views.py`, `schedules.py`, `udfs.py`, `masks.py`, `notebooks.py`, `variables.py`. Exposed via `api.Deploy`. |
| `utils/` | Generic helpers with no Fabricks-domain knowledge (path handling, Spark session helpers, pip listing, etc.) — safe to depend on from anywhere. |

## Request flow (schedule → tables)

1. A Databricks job runs `api/notebooks/run` (or `process`/`standalone`) for a
   named schedule.
2. `context` has already parsed the runtime YAML at import time into
   `CONF_RUNTIME` and the step registries.
3. `core/schedules` resolves the schedule to its jobs/views;
   `core/dags.DagGenerator` builds the dependency graph across those jobs
   (parents from each job's `options.parents`).
4. `core/dags.DagProcessor` walks the DAG (respecting configured
   concurrency/`workers`) and, per job, drives `core/jobs` — configure the
   job from its YAML + step defaults (`base/configurator.py`), generate its
   SQL (`base/generator.py`), run pre-checks, hand off to `cdc/` for the
   configured `change_data_capture` mode to build/merge the target, run
   post-checks (`base/checker.py`), and either write to `metastore/` or, for
   `invoke` jobs, run a notebook (`base/invoker.py`).
5. `core/dags.DagTerminator` finalizes run state/logging once the graph is
   exhausted.

## Runtime vs. framework code

Two different codebases share the term "runtime":

- **This repo** (`framework/fabricks/`) is the framework — versioned,
  published as the `fabricks` PyPI package.
- **A runtime** is a *consumer* repo: YAML job configs plus SQL/notebook
  files under `bronze/`, `silver/`, `gold/`, pointed to by that repo's
  `[tool.fabricks]` config in `pyproject.toml`. `tests/spark/databricks/runtime/`
  is this repo's own fixture runtime, used only by the integration suite —
  see [TEST.md](./TEST.md).

Changes to `models/` or `context/config.py` change what a runtime's YAML is
allowed to say; changes to `core/` change how that YAML gets executed. Keep
that distinction in mind — a "simple" tweak in `core/jobs/base` can be a
breaking change for every runtime repo, not just this one.

## Where to look for detail

- End-user pipeline concepts (bronze/silver/gold, CDC config, checks,
  extenders/UDFs/parsers) — `framework/README.md`.
- Coding rules — [CONSTITUTION.md](./CONSTITUTION.md).
- Test layout and how to run tests — [TEST.md](./TEST.md).
- Known hard-won bug signatures — [DEBUG.md](./DEBUG.md).
