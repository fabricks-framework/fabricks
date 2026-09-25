---
name: fabricks-options
description: Use when setting or reviewing a Fabricks job's table_options (clustering/partitioning choice, masks, primary_key/foreign_keys, powerbi), check_options (data-quality checks), invoker_options (pre/post-run notebooks), register_options, or options.script. Not for core job shape (mode, parents) or CDC (change_data_capture) — see fabricks / fabricks-cdc for those.
---

# Fabricks Secondary Options

Job config beyond the core shape (`step`/`topic`/`item`/`mode`/`parents`,
covered in `fabricks`): the physical table, data-quality checks, and
pre/post-run notebooks.

**REQUIRED BACKGROUND:** `fabricks` for the overall job config shape.

## `table_options`

Configures the physical Delta table Fabricks builds for a job —
clustering, retention, masking, constraints — separate from `options`
(job behavior) or `check_options` (data checks).

```yaml
table_options:
  liquid_clustering: true
  cluster_by: [customer_id, __source] # mutually exclusive with partition_by/zorder_by
  retention_days: 30 # VACUUM/time-travel retention
  masks:
    email: mask_email # column -> registered mask UDF (column-level security)
  primary_key:
    pk:
      keys: [customer_id]
  properties:
    delta.autoOptimize.optimizeWrite: true # arbitrary Delta TBLPROPERTIES
```

| Option | Use for |
|---|---|
| `liquid_clustering` + `cluster_by` | Preferred over partition/zorder on new tables (Databricks' recommendation) |
| `partition_by` / `zorder_by` | Classic partitioning/z-order, when liquid clustering isn't an option |
| `masks` | Column -> mask function name, for column-level security |
| `primary_key` / `foreign_keys` | Informational constraints (not enforced by Delta, but declared for lineage/BI tools) |
| `powerbi` | Sets Delta protocol/column-mapping properties for PowerBI's `fn_ReadDeltaTable` direct-file reader (see below) |

The full field list (`bloomfilter_by`, `retention_days`, `properties`,
`identity`, `comment`, ...) is in `job-schema*.json` at the repo root —
self-explanatory from the field name, with editor autocomplete via each
YAML's `$schema:` comment.

`powerbi: true` sets Delta reader/writer-version and column-mapping
properties needed for PowerBI's `fn_ReadDeltaTable` M-function
(https://github.com/JohnMichaelR/delta-io-connectors/blob/master/powerbi/fn_ReadDeltaTable.pq),
which reads Delta files directly from storage (bypassing Databricks
compute) — it isn't part of Fabricks. **This is a legacy/direct-access
path.** The preferred way to load a Fabricks table into PowerBI is the
SQL warehouse via the Databricks connector, which doesn't need
`powerbi: true` at all. Reach for `powerbi: true` only when a report
specifically needs direct-file access instead of going through a
warehouse.

### Step-Level Defaults

A subset (`powerbi`, `liquid_clustering`, `properties`, `retention_days`,
`masks`) can be set on the step itself as a default for every job in that
step. A job's own `table_options` overrides it per-field, not wholesale —
setting one field in a job doesn't reset the others back to unset.

## `check_options`

Checks are how you test your data, not just your SQL syntax — use them to
catch a bad load before it reaches consumers, the same way you'd write a
unit test for code. `min_rows`/`max_rows`/`count_must_equal` cover simple
shape checks; `pre_run`/`post_run` let you write an arbitrary SQL
assertion, e.g. "this year's sales total shouldn't have dropped outside a
plausible range compared to history":

```yaml
check_options:
  min_rows: 10 # table must have >= N rows
  max_rows: 10000 # table must have <= N rows
  count_must_equal: fabricks.other_table
  pre_run: true # runs <item>.pre_run.sql before the load
  post_run: true # runs <item>.post_run.sql after the load
```

```sql
-- gold/sales/daily_summary.post_run.sql
select
    'sales total outside plausible range' as __message,
    'fail' as __action
from gold.sales_daily_summary
where total_amount not between (select avg(total_amount) * 0.5 from gold.sales_daily_summary)
                            and (select avg(total_amount) * 2 from gold.sales_daily_summary)
```

A `pre_run`/`post_run` script returns `__message` (shown in logs) and
`__action` (`'fail'` or `'warning'`). A `'fail'` restores the table to its
pre-run version — the write is discarded — unless the table is `memory`
(view-only), where only the error is logged.

## `invoker_options`

Run a notebook before/after a job, e.g. to hit an API or trigger
downstream automation:

```yaml
invoker_options:
  pre_run:
    - notebook: bronze/invokers/http_call
      arguments:
        url: https://example.com
      warn_on_error: true # log + continue instead of failing the run on invoker failure
```

`warn_on_error` (default unset = fails the run like any other error) is
per-invoker-entry and works on both `pre_run` and `post_run`.

## `register_options`

**Gold-only** — a distinct mechanism from bronze's own `mode: register`
(which uses `options.uri`, no `register_options`, and no relation to
this at all). On a gold job, `mode: register` + `register_options`
builds the table straight from a pre-existing file
(`select * from {file_format}.\`{uri}\``), no SQL file, no invoker:

```yaml
- job:
    step: gold
    topic: sales
    item: dim_customer
    options:
      mode: register
    register_options:
      uri: abfss://.../dim_customer.parquet
      file_format: parquet
```

## `options.script`

`script: true` marks a gold job's `.sql` file as a multi-statement
script (e.g. `create table ...; insert into ...`) instead of the usual
single query — needed if the job does more than one DDL/DML statement.
`fabricks.runchecks` validates it accordingly (parses the whole file,
not just the first statement, and doesn't require it to be a single
`select`).

## Common Mistakes

- Setting both `cluster_by` and `partition_by`/`zorder_by` — they're
  mutually exclusive strategies, pick one.
- Expecting `primary_key`/`foreign_keys` to be enforced — Delta doesn't
  enforce them; they're metadata for lineage/BI tooling only.
- Setting `liquid_clustering` on an existing table already partitioned —
  switching strategies on a live table needs a rewrite, it's not a
  transparent in-place change.
- Relying on a `post_run` check to leave the write in place after a
  `'fail'` — it doesn't (table is restored), unless `mode: memory`.
