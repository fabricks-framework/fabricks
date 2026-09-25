---
name: fabricks
description: Use when writing or reviewing a Fabricks runtime repo (YAML job configs under bronze/silver/gold, or the SQL behind them) — for project setup, step layout, or any "how do I configure a Fabricks job" question. Not for working on the Fabricks framework's own source code.
---

# Fabricks

## Overview

Fabricks turns declarative YAML job config + SQL into scheduled Databricks
table builds. A **runtime** repo (the thing this skill is for) contains only
YAML, SQL, and notebooks — orchestration, CDC, and table lifecycle are owned
by the framework.

For building an SCD1 or SCD2 job specifically, use `fabricks-cdc`. For
`table_options`, `check_options`, `invoker_options`, `register_options`,
or `options.script`, use `fabricks-options`. For project setup
(`fabricksconfig.json`/`pyproject.toml`, the main conf YAML, schedules,
`tags`, `options.type`/`wait_for`, or offline validation with
`fabricks.runchecks`), use `fabricks-project`. For the Python API
(`get_job`/`get_step`/`Deploy`, the scheduled-pipeline shape, or
scheduling maintenance), use `fabricks-api`. This skill covers the core
job shape.

## Pipeline Shape

Four step categories in the main conf YAML (`bronze:`, `silver:`,
`gold:`, `powerbi:`), each a list of one or more step names — a runtime
can have more than one step of the same category (e.g. `gold:` listing
both `gold` and `transf`). `step` in a job config is that step *name*,
not the category itself.

| Category | Purpose | Typical `mode` |
|---|---|---|
| bronze | Raw ingestion | `register` |
| silver | Conform/clean, optional CDC (SCD1/SCD2) | `update`, `latest`, `combine` |
| gold (and other gold-shaped steps, e.g. `transf`) | Consumption-ready SQL, optional CDC | `complete`, `update` |
| powerbi | Its own conf category, gold-shaped in practice (same SQL-authoring rules) | `complete`, `update` |

Each job is one `job:` entry in a `_config.*.yml` file under the step's
`path_options.runtime` directory, keyed by `step` + `topic` + `item`.
**Gold is the only step where you write SQL** (`<item>.sql`, a sibling of
the job's YAML). Bronze and silver are pure config — the only code you
write for them is an invoker notebook or an extender, never a `.sql`
file for the job itself.

## Bronze

The preferred (and only recommended) bronze pattern is `mode: register`
with a `pre_run` invoker notebook that loads the source data into the
`uri` Delta table itself — Fabricks then just registers that table:

```yaml
- job:
    step: bronze
    topic: sales
    item: daily_transactions
    options:
      mode: register
      uri: abfss://.../raw/sales
      keys: [transaction_id]
    invoker_options:
      pre_run:
        - notebook: bronze/invokers/sales_api
```

`mode: append` + `parser:` (having Fabricks itself parse raw files) is
legacy — don't use it for new jobs.

`__key` is derived automatically from `keys:` (a hash of those columns)
if the source doesn't already have a `__key` column — you don't add it
yourself. `__operation` defaults to `'upsert'` for the whole batch if
absent. Neither of those needs the invoker notebook to add it explicitly.

## Silver

Silver has **no SQL file at all** — Fabricks reads `select * from
{parent}` itself; there's nothing to author beyond YAML (and, rarely, an
extender). The *coded* default for `change_data_capture` is `nocdc` (a
plain passthrough, no key/history tracking) — but in practice a real
silver job almost always sets it explicitly: `scd2` is the typical
choice, `scd1` for current-state-only. (A fourth value, `scd0`, also
exists but is rare — check the framework's own CDC code before reaching
for it.)

```yaml
- job:
    step: silver
    topic: sales
    item: daily_transactions
    options:
      mode: update
      change_data_capture: scd2
```

`parents:` is omitted: silver's default parent is
`{parent_step}.{topic}_{item}`, here `bronze.sales_daily_transactions`.

Because there's no SQL to compute `__timestamp`/`__operation`, those
columns must already exist on the **parent** table by the time silver
reads it. Preferred: the bronze `pre_run` invoker notebook loads them
directly into the Delta table alongside the business columns. Less
preferred: add them afterward via an extender — an extra transform step
outside the declarative config, more indirection to trace through. See
`fabricks-cdc` for the full column contract and per-mode `deduplicate`
defaults.

Fabricks automatically creates `<table>__current` for every **silver**
table (bronze and gold don't get one) — a view over the base table,
filtered to `where __is_current` when that column exists (i.e. the job
uses SCD1/SCD2), or unfiltered otherwise. Not gated on CDC choice: any
silver job gets it. Prefer it over the base table when you only want live
rows.

## Gold

Gold is where you actually write SQL. Plain (non-CDC) example below —
see `fabricks-cdc` for `change_data_capture: scd1`/`scd2`:

```yaml
- job:
    step: gold
    topic: sales
    item: daily_summary
    options:
      mode: complete
```

```sql
-- gold/sales/daily_summary.sql
select
    day(order_date) as day,
    sum(amount) as total_amount
from silver.sales_daily_transactions__current
group by day(order_date)
```

`register_options` and `options.script` are covered in `fabricks-options`.

For project setup (`fabricksconfig.json`/`pyproject.toml`, the main conf
YAML, schedules, `tags`, `options.type`/`wait_for`) and offline
validation (`python -m fabricks.runchecks`), see `fabricks-project`.

