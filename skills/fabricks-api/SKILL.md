---
name: fabricks-api
description: Use for Fabricks's Python API — get_job/get_step and their methods (run, create, drop, maintain, ...), Deploy (deploying schedules/views/udfs after config changes), the generic scheduled-pipeline workflow shape (cluster/initialize/process-per-step/terminate), and scheduling a maintenance sweep (vacuum/optimize/compute_statistics) across all jobs. Not for job YAML/SQL authoring — see fabricks / fabricks-cdc / fabricks-options for that.
---

# Fabricks Python API & Operations

**REQUIRED BACKGROUND:** `fabricks` for job config basics; `fabricks-project`
for `Deploy.schedules()`'s relationship to `schedules.yml`.

## `get_job` / Job Methods

```python
from fabricks.api import get_job

job = get_job(job="gold.sales_daily_summary")       # "step.topic_item" string
job = get_job(step="gold", topic="sales", item="daily_summary")
job = get_job(step="gold", job_id="<job_id>")
```

`get_job` itself is the public, exposed API — the methods below are
**not**: they're internal `Bronze`/`Silver`/`Gold` implementation
methods, not a documented/stable contract. They're callable, and used by
ops/admin scripts that need to drive an individual job directly (a
backfill, a manual re-run, a maintenance sweep), but treat them as
internal — not something to build application logic on top of.

| Method | Does |
|---|---|
| `run()` | The normal incremental run |
| `for_each_run()` | The per-run orchestration body `run()` calls (view-vs-table handling, data fetch, write) — not per-batch despite the name; for a streaming job it sets up the `foreachBatch` write stream that then invokes the batch body itself multiple times |
| `create()` | Creates the table/view if missing |
| `drop()` | Drops the table/view |
| `register()` | For a table-mode job, registers an existing Delta table as this job's table; for a view-mode job (e.g. `mode: memory`/`combine`), it instead recreates the view (`create_or_replace_view()`) |
| `truncate()` | Empties the table |
| `overwrite()` | Full reprocess, ignoring incremental state |
| `update_schema()` / `overwrite_schema()` | Evolve vs. replace the table schema |
| `invoke_pre_run()` / `invoke_post_run()` | Run this job's `invoker_options` notebooks on demand |
| `check_pre_run()` / `check_post_run()` | Run this job's `check_options` SQL on demand |
| `maintain(vacuum=, optimize=, compute_statistics=)` | Table maintenance — see "Scheduling Maintenance" below |
| `register_udfs()` | Gold only |
| `update_dependencies()` | Refresh this job's row in the dependency table |

## `get_step` / Step Methods

```python
from fabricks.api import get_step

step = get_step(step="gold")
```

Same caveat as job methods: `get_step` is the public API, its methods
below are internal `BaseStep` implementation for ops/admin tooling, not
application logic.

| Method | Does |
|---|---|
| `create()` | Thin wrapper around `update()` |
| `update(update_dependencies=True, incremental=False, ...)` | Full step refresh |
| `create_db_objects(...)` | Creates every job's table/view in this step (deprecated alias: `create_jobs`) |
| `update_dependencies(...)` | Recomputes the dependency graph for this step's jobs |
| `update_configurations(drop=False)` | Reloads job config from YAML into the metastore (deprecated alias: `update_jobs`) |
| `update_tables_list()` / `update_views_list()` | Refresh the step's cached table/view lists |
| `register(update=False, drop=False)` | Registers the step itself |
| `get_jobs()` / `get_jobs_iter()` | List the step's jobs |
| `get_dependencies()` | The step's dependency DataFrame |

There's no step-level `maintain()` — maintenance is per-job (see below).

## `Deploy`

`from fabricks.api.deploy import Deploy` — a class of `@staticmethod`s,
called as `Deploy.method()` (never instantiated):

| Method | Deploys |
|---|---|
| `Deploy.tables(drop=False, update=False)` | The framework's own `fabricks` schema tables (steps, logs, dummy) |
| `Deploy.views()` | The framework's own `fabricks` schema views (jobs, tables, dependencies, schedules, ...) |
| `Deploy.udfs(overwrite=True)` | UDFs |
| `Deploy.masks(overwrite=True)` | Masking functions |
| `Deploy.notebooks(overwrite=False)` | Uploads Fabricks notebooks to the workspace |
| `Deploy.schedules()` | **Makes `schedules.yml` changes effective** — the required call after editing a schedule |
| `Deploy.variables()` / `Deploy.runtime()` | Runtime/variable config |
| `Deploy.step(step)` | Convenience: `tables()` → `get_step(step).create()` → `views()` → `schedules()` |
| `Deploy.job(step)` | Just `get_step(step).create()` |
| `Deploy.armageddon(steps=None, nowait=False)` | Full teardown + rebuild of everything — destructive, drops the `fabricks` db and storage first |

## The Scheduled Pipeline Shape

A Fabricks pipeline's Databricks Job is always the same shape: a no-op
**cluster warm-up** task, an **initialize** task, one **process** task
per step (parallel where the DAG allows), and a **terminate** task gated
on all process tasks finishing:

```
cluster (no-op, just spins up the job cluster early)
  └─ initialize
       ├─ process(step: bronze)
       ├─ process(step: silver)
       ├─ process(step: gold)
       └─ ... (one per step, dependencies mirror bronze→silver→gold)
            └─ terminate (run_if: ALL_DONE)
```

Backed by three `fabricks.core.schedules` functions, each wrapped in its
own template notebook shipped with the framework
(`fabricks/api/notebooks/{initialize,process,terminate,cluster}.py` —
copy these into your runtime's notebook directory, per the framework
README's setup instructions):

```python
# initialize.py
from fabricks.core.schedules import generate
_, job_df, dependency_df = generate()  # reads the "schedule" widget

# process.py (one task per step, base_parameters={"step": "<name>"})
from fabricks.core.schedules import process
process()  # reads step/schedule/schedule_id widgets

# terminate.py (depends_on every process task, run_if: ALL_DONE)
from fabricks.core.schedules import terminate
terminate()  # reads the schedule_id widget
```

Define the actual Databricks Job/task graph wiring these together with a
**Databricks Asset Bundle** (`databricks.yml` + a `resources/*.yml` job
definition) — that's the standard, version-controlled way to deploy it,
not a one-off UI-created job.

`terminate` is also where `DagTerminator` raises if any job failed — a
false-positive edge case exists around transient invoker errors that
recover before the schedule ends (see [fabricks-framework/fabricks#189](https://github.com/fabricks-framework/fabricks/issues/189)).

## Ad-Hoc Job/Step Operations

For one-off or bulk operations outside the normal schedule (backfills,
re-running a failed job, mass table maintenance), write a small
dispatcher notebook: `dbutils.widgets` for which action(s), which
job(s)/step(s), and worker count; resolve the widget values to
`get_job`/`get_step` method calls; run them with a `ThreadPoolExecutor`
for parallelism. Job selection can support a SQL `ilike` wildcard against
`fabricks.jobs` (e.g. `jobs = "gold.sales_%"` or `jobs = "%"` for every
job) and comma/bracket-separated lists, so a single run can target
anything from one job to the whole runtime.

## Scheduling Maintenance

Fabricks doesn't auto-vacuum/optimize/compute-statistics — nothing does
this automatically unless you schedule it. The idiom: schedule the same
kind of ad-hoc dispatcher notebook above, periodically (e.g. weekly),
targeting every job (`jobs="%"`) with `job.maintain(vacuum=True,
optimize=True, compute_statistics=True)`. Skipping this doesn't break
correctness, but table performance and storage cost both degrade over
time as small files and stale statistics accumulate — treat it as a
required, not optional, scheduled job.

## Common Mistakes

- Editing `schedules.yml` and expecting it to take effect without
  running `Deploy.schedules()` — it won't; nothing watches the file.
- Calling `get_step(step).create()` expecting it to pick up YAML changes
  — `create()` only creates what's missing; use `update_configurations()`
  (or `Deploy.step(step)`, which chains the right calls) to reload
  config. Root cause: with the (default) `job_config_from_yaml: false`
  setting, jobs read from the deployed metastore table, not the YAML
  files — see `fabricks-project`.
- With `job_config_from_yaml: true`, editing a YAML file mid-session and
  expecting the next `get_job`/`get_step` call to see it — YAML reads are
  cached per-process; restart the Python process first (see
  `fabricks-project`).
- Never scheduling a maintenance sweep at all — it's not automatic.
- Reaching for `Deploy.armageddon()` for anything short of "wipe and
  rebuild this environment from scratch" — it's destructive and drops
  the `fabricks` db/storage first.
