---
name: fabricks-cdc
description: Use when adding or reviewing a Fabricks silver or gold job with change_data_capture set to scd1 or scd2, or any job producing __key/__timestamp/__operation rows for a CDC merge — including deciding between SCD1 (current-state) and SCD2 (versioned history), and aggregating one SCD2 table on top of another.
---

# Fabricks CDC (SCD1 / SCD2)

## Overview

Both SCD1 and SCD2 merge source rows into a target table via the same
`__key`/`__timestamp`/`__operation` contract — they differ only in what
the target keeps:

| | Keeps | Adds |
|---|---|---|
| SCD1 | Only the **current state** per key | `__is_current`, `__is_deleted` |
| SCD2 | **Every version** over time | `__valid_from`, `__valid_to`, `__is_current`, `__is_deleted` |

Use SCD1 when you need "what is true now, including whether it was
deleted." Use SCD2 only when a downstream consumer needs "what was true
as of date X" — it costs more storage and merge time than SCD1 for no
benefit if nobody ever queries a past state.

**REQUIRED BACKGROUND:** `fabricks` for job config basics — in particular,
that gold is the only step where you write SQL; silver is pure config,
always `select * from {parent}` under the hood.

## The Column Contract

Whatever produces the rows a SCD1/SCD2 merge reads — gold's own SQL, or
the parent table a config-only silver job selects from — needs three
system columns (plus a fourth, optional one), and whatever data fields
the table needs:

| Column | Meaning |
|---|---|
| `__key` | Unique key for the row (any type) — auto-derived on bronze from `keys:` if absent |
| `__timestamp` | When the row changed or was deleted |
| `__operation` | `'upsert'`, `'delete'`, or `'reload'` |
| `__source` | Optional — which upstream feed the row came from, when a target merges more than one |

**On gold**, you write this yourself:

```sql
select
    customer_id as __key,
    name,
    email,
    updated_at as __timestamp,
    if(is_deleted, 'delete', 'upsert') as __operation
from raw.customers
```

**On silver**, there's no SQL to write — `__timestamp`/`__operation` must
already be columns on the **parent** table by the time silver's implicit
`select * from {parent}` runs. Preferred for new jobs: the bronze
`pre_run` invoker notebook loads them directly alongside the business
columns. **Legacy, but still common** in repos not yet migrated off it:
an `extender_options` entry adds them afterward instead — Fabricks ships
no built-in extenders, so this means a project-specific extender
function (naming and behavior vary per runtime). It works, and you'll
see it a lot in older bronze configs, but it's an extra transform step
outside the declarative config (more indirection to trace through) —
prefer the invoker for anything new.

`'reload'` marks a row as the full current state for its key without
distinguishing an actual change from a delete — used for reconciliation
batches (e.g. `delete_missing`) rather than everyday incremental rows. On
**gold** with `mode: update`, if the query has no `__operation` column at
all, Fabricks synthesizes `__operation = 'reload'` for the whole result
set automatically. On **bronze**, `__operation` defaults to `'upsert'`
for the whole batch if the invoker doesn't provide it.

SCD2 additionally derives `__valid_from`/`__valid_to`/`__is_current` from
consecutive `__timestamp`s per `__key` — never set those columns yourself.

**`__source`** is only needed when one target merges rows from **more
than one upstream feed with independent change cadences** (typically
`mode: combine`). On gold, add it as a literal per `union` branch:

```sql
select customer_id as __key, ..., updated_at as __timestamp, 'crm' as __source
from silver.crm_customers
union all
select customer_id as __key, ..., changed_at as __timestamp, 'billing' as __source
from silver.billing_customers
```

On bronze, set `options.source: crm` instead —
same auto-fill pattern as `__operation`/`options.operation`, a no-op if
the invoker/parser already produced `__source` itself.

**On silver `mode: combine`, you don't need to set it at all** —
Fabricks auto-fills `__source` per parent as `'{parent}' as __source`
(e.g. `'bronze.crm_customer'`) for any parent that doesn't already have
it. A silver job with `parents:` under any *other* mode is stricter:
`__source` must already exist on every parent if there's more than
one, or it hard-fails.

Without it, Fabricks tracks one global `max(__timestamp)` for
incremental pulls, so a busy feed (`crm`) drags the watermark past a
quiet feed's (`billing`) genuinely-new-but-older rows, silently
skipping them. With `__source`, the watermark and the target-table
`__current` cache are both tracked per-source instead, and on bronze
it also joins the `__key`/`__hash` computation so identical natural
keys from different feeds don't collide into one row.

## Job Config

```yaml
- job:
    step: silver
    topic: crm
    item: customer
    options:
      mode: update
      change_data_capture: scd1 # or scd2
```

- `mode: update` — incremental merge into the target (the common case).
  `mode: complete` reprocesses the full source each run.
- `parents` — omitted here on purpose: when unset, a silver job's default
  parent is exactly `{parent_step}.{topic}_{item}` (here, `bronze.crm_customer`)
  — the framework derives it, don't declare `parents:` unless the real
  parent differs from that. `mode: combine` is the standard case where it
  does: combine merges more than one source table, so there's no single
  implicit parent to fall back to — declare `parents:` explicitly there.
- `deduplicate` — defaults to `true` on **silver** for any mode other than
  `append` (covers `update`/`latest`/`combine`, i.e. every SCD1/SCD2 mode),
  so you don't need to set it there. On **gold**, it defaults to `false`:
  gold SQL is expected to already produce one row per `__key`/`__timestamp`
  (the author writes the aggregation/grouping), and skipping the dedup
  pass is a deliberate performance choice, not an oversight. Only set
  `deduplicate: true` on a gold job when you genuinely can't guarantee
  that uniqueness in the SQL itself.
- `hard_delete: true` (**gold only**) makes a `'delete'` physically
  remove the row instead of soft-deleting (flipping `__is_deleted`).
  There's no equivalent option on silver — a silver SCD1/SCD2 job always
  soft-deletes, unconditionally, not configurable.
- `correct_valid_from: false` (**gold only**, SCD2) — set this to opt
  OUT of Fabricks rewriting the earliest version's `__valid_from` to a
  sentinel low date; leave unset/true for normal "since forever"
  semantics on the first version. On silver, this is always on and the
  option is ignored if set — silver SCD2 doesn't read it at all.

## Distributing SCD1 To An External System

A common idiom: a gold `mode: update` + `change_data_capture: scd1` job
with `persist_last_timestamp: true`, paired with a `post_run` invoker
that pushes the changed rows to an external system (an API, a search
index):

```yaml
options:
  mode: update
  change_data_capture: scd1
  persist_last_timestamp: true
invoker_options:
  post_run:
    - notebook: distribution/external_system/upload
```

`persist_last_timestamp` keeps track of the last-processed `__timestamp`
across runs so the `post_run` invoker can push only what changed since
last time, instead of the whole table.

## Resulting Table Shape

SCD1 and SCD2 merges both add `__is_current`/`__is_deleted` to the target
table, on silver or gold alike. For the `<table>__current` convenience
view built from those columns, see `fabricks` — it's a general silver
behavior (created for every silver table, not gated on CDC choice), not
something specific to SCD1/SCD2, and it isn't created for gold at all.

## Aggregating One SCD2 Table On Top Of Another

The hard part isn't the merge — it's re-deriving `__key`/`__timestamp`/
`__operation` when your source's grain changes (e.g. rolling stock
movements up to an article/location-level SCD2). Two things to get right:

1. A row is "closed" (no longer feeds the aggregate) when the *next*
   version at the source grain moves to a **different aggregate key**, not
   only when the source itself is deleted — a `lead()` window per source
   key, ordered by `__valid_from`, catches both.
2. Whether closing emits a real `'delete'` or an `'upsert'` with the
   measure zeroed is a business choice: a real delete drops the aggregate
   row (`__is_current`/`__is_deleted` flip); a zeroed upsert keeps the row
   visible with value 0. Production Fabricks pipelines commonly use the
   latter so downstream consumers don't have to special-case a vanished
   key.

```sql
with
    newkey as (
        select concat_ws('*', article_id, location_id) as __key, *
        from silver.scd2_stock
    ),
    root as (
        select
            -- closed if the next version (any key) has a different aggregate key, or there is none
            if(
                coalesce(lead(__key) over (partition by source_key order by __valid_from), __key) = __key,
                __is_deleted,
                true
            ) as __closed,
            *
        from newkey
    ),
    events as (
        select article_id, location_id, __valid_from as __timestamp, 'upsert' as __operation
        from root
        union
        select article_id, location_id, __valid_to as __timestamp, 'delete' as __operation
        from root
        where __closed
    )
select
    concat_ws('*', e.article_id, e.location_id) as __key,
    e.article_id,
    e.location_id,
    e.__timestamp,
    'upsert' as __operation, -- zeroed-upsert instead of a real delete; see point 2 above
    sum(if(e.__operation = 'delete' and e.__timestamp = r.__valid_to, 0, r.qty)) as qty
from events e
inner join newkey r
    on e.__timestamp between r.__valid_from and r.__valid_to
    and e.article_id = r.article_id
    and e.location_id = r.location_id
group by e.article_id, e.location_id, e.__timestamp
```

The `if(operation = 'delete' and __timestamp = __valid_to, 0, ...)`
guard excludes the row's own closing value from the sum — without it, a
closing event still includes the value it's supposed to be zeroing out.

## Common Mistakes

- Looking for a `.sql` file to author `__timestamp`/`__operation` logic
  for a silver SCD1/SCD2 job — there isn't one. Those columns have to
  already be on the parent table (via the bronze invoker, preferably) or
  added via an extender.
- Writing a gold job's SQL without guaranteeing one row per
  `__key`/`__timestamp` and leaving `deduplicate` unset — gold assumes
  the SQL already did that (performance choice), so a batch with >1 row
  per key silently produces an undefined/incorrect merge instead of
  erroring. Either fix the SQL's grouping (the aggregation example's
  `group by e.article_id, e.location_id, e.__timestamp` is what makes
  `deduplicate` unnecessary there) or set `deduplicate: true`.
- Forgetting `__operation` must be exactly `'upsert'`, `'delete'`, or
  `'reload'` — anything else is a config/SQL bug, not a Fabricks bug.
- Re-deriving `__valid_from`/`__valid_to`/`__is_current` yourself in an
  SCD2 source query — Fabricks computes these from `__timestamp`; setting
  them in your SELECT is either ignored or conflicts with the merge.
- Using SCD2 when SCD1 is all the consumer actually needs — SCD2 costs
  more storage and merge time for no benefit if nobody ever queries "as
  of a past date."
- Expecting history from SCD1 — it overwrites in place. If you later need
  "what was this row's value last month," that's SCD2, not SCD1.
- Detecting a closed aggregate row only via the source row's own
  `__is_deleted`, missing the case where the row survives but moves to a
  different aggregate key (the `lead()` check above catches both).
