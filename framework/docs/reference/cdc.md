# Change Data Capture (CDC)

Fabricks provides built-in Change Data Capture (CDC) patterns to keep downstream tables synchronized with upstream changes using SQL-first pipelines. CDC is enabled per job via `options.change_data_capture` and implemented by generated MERGE/INSERT statements driven by small helper columns.

This page explains the supported CDC strategies, required inputs, merge semantics, and examples.

---

## Strategies

| Strategy | Description                                                                                   | Convenience views                  |
|---------|-----------------------------------------------------------------------------------------------|-------------------------------------|
| `nocdc` | No CDC; writes the result as-is.                                                              | —                                   |
| `scd1`  | Tracks current vs deleted; maintains flags `__is_current`, `__is_deleted`.                    | `{table}__current` in Silver        |
| `scd2`  | Slowly Changing Dimension Type 2: validity windows with `__valid_from`, `__valid_to`.         | `{table}__current` in Silver        |

### What is SCD1?
- Definition: Slowly Changing Dimension Type 1 keeps only the current state of each business key. Attribute changes overwrite previous values rather than preserving history.
- Typical columns in Fabricks: `__is_current`, `__is_deleted`. A convenience view `{table}__current` in Silver selects current non-deleted rows.
- When to use: When downstream consumers only need the latest values and you do not need to answer “as-of” questions or audit historical attribute values.

### What is SCD2?
- Definition: Slowly Changing Dimension Type 2 preserves the full change history by creating a new versioned row each time attributes change. Each row covers a validity window for a given business key.
- Typical columns in Fabricks: `__valid_from`, `__valid_to`, and `__is_current` (optional `__is_deleted` if soft-deletes are modeled). Silver also provides `{table}__current` for latest rows.
- When to use: When you must answer “as-of” queries (e.g., “What was the customer segment on 2024‑03‑01?”), analyze changes over time, or maintain auditable history. In Gold SCD2 merges, inputs use `__key`, `__timestamp`, `__operation` to define change points.

---

## How to enable CDC

Set the CDC strategy in the job options:

```yaml
- job:
    step: silver
    topic: demo
    item: scd1
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.demo_source]
```

For Gold:

```yaml
- job:
    step: gold
    topic: scd2
    item: update
    options:
      mode: update
      change_data_capture: scd2
```

Supported values: `nocdc`, `scd1`, `scd2`.

---

## Input contracts

Some helper columns govern CDC behavior. Fabricks generates additional internal helpers during processing.

- Gold jobs (consumer side):
    - scd2 (required): `__key`, `__timestamp`, `__operation` with values `'upsert' | 'delete' | 'reload'`.
    - scd1 (required): `__key`; optional `__timestamp` / `__operation` (`'upsert' | 'delete' | 'reload'`) for delete/rectify handling.
    - Note: If `__operation` is absent in Gold SCD update jobs, Fabricks auto-injects `__operation = 'reload'` and enables rectification.
    - Optional helpers used by merges:
        - `__order_duplicate_by_asc` / `__order_duplicate_by_desc`
        - `__identity` (only when `table_options.identity` is not true; if `identity: true`, the identity column is auto-created and you should not supply `__identity`)
        - `__source` (to scope merges by logical source)
        
- Silver jobs (producer side):
    - Provide business keys through job-level `keys` (or compute a `__key`) to support downstream CDC.
    - Silver can apply CDC directly and yields convenience views (e.g., `{table}__current`).

Note

- Memory outputs ignore columns that start with `__`.
- Special characters in column names are preserved.

See details in:

- Steps: [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- [Table Options](./table-options.md)

---

## Merge semantics (under the hood)

Fabricks compiles CDC operations into SQL via Jinja templates at runtime. The core logic lives in `fabricks.cdc`:

- `Merger.get_merge_query` renders `templates/merge.sql.jinja` for the selected `change_data_capture` strategy.
- The framework computes internal columns such as:
    - `__merge_condition` — one of `'upsert' | 'delete' | 'update' | 'insert'` depending on strategy and inputs.
    - `__merge_key` — a synthetic key used to join against the target.
- You usually do not set these internal fields manually; they are derived from your inputs (`__key`, `__operation`, `__timestamp`) and job options.

*Join keys*

- If a `__key` column exists in the target, merges use `t.__key = s.__merge_key`.
- Otherwise, the configured `keys` option is used to build an equality join on business keys.

*Source scoping*

- If `__source` exists in both sides, merges add `t.__source = s.__source` to support multi-source data in the same table.

*Soft delete vs hard delete (SCD1)*

- If the incoming data contains `__is_deleted`, the SCD1 template performs soft deletes:
    - Sets `__is_current = false`, `__is_deleted = true` on delete.
- If `__is_deleted` is absent, deletes are physical for SCD1.

*Timestamps and metadata*

- If the incoming data provides `__timestamp`, it is propagated to the target.
- If the target has `__metadata`, the `updated` timestamp is set to the current time during updates/deletes.

*Identity and hash*

- If `table_options.identity: true`, the identity column is created automatically when the table is created.
- If `table_options.identity` is not true and `__identity` is present in the input, it will be written as a regular column.
- If `__hash` is present, it is updated during upsert operations.

*Update filtering*

- `options.update_where` can constrain rows affected during merges (useful for limiting the scope of updates).

*Internals reference*

- `framework/fabricks/cdc/base/merger.py`
- Templates under `framework/fabricks/cdc/templates/merge/*.sql.jinja`

---

## SCD1 details

*Behavior* (see `merge/scd1.sql.jinja`)

- Upsert (`__merge_condition = 'upsert'`): updates matching rows and inserts non‑matching rows.
- Delete (`__merge_condition = 'delete'`):
    - Soft delete if `__is_deleted` is part of the schema: sets `__is_current = false`, `__is_deleted = true`.
    - Otherwise, performs a physical delete.

*Convenience view (in Silver)*

- `{table}__current`: filters current (non‑deleted) rows for simplified consumption.

*Minimal Silver example*

```yaml
- job:
    step: silver
    topic: monarch
    item: scd1
    options:
      mode: update
      change_data_capture: scd1
```

*Gold consumption example*

```sql
-- Example: consuming current rows from SCD1 silver output
select id as id, name as monarch
from silver.monarch_scd1__current
```

---

## Reload operation

`'reload'` is a CDC input operation used as a reconciliation boundary. It is not a direct MERGE action; instead, it signals Fabricks to rectify the target table using the supplied snapshot at that timestamp.

- Purpose: mark a full or partial snapshot boundary so missing keys can be treated as deletes and present keys as upserts as needed.
- Auto-injection: when a Gold SCD job runs in `mode: update` and your SQL does not provide `__operation`, Fabricks injects `__operation = 'reload'` and turns on rectification.
- **Silver** behavior:
      - If a batch contains `'reload'` after the target’s max timestamp, Silver enables rectification logic.
      - In `mode: latest`, `'reload'` is not allowed and will be rejected.
- **Gold** behavior:
      - Passing `reload=True` to a Gold job run triggers a full `complete` write for that run.
- Internals: rectification is computed in `framework/fabricks/cdc/templates/query/rectify.sql.jinja`, which computes next operations/windows around `'reload'` markers.

!!! warning
    In Silver `mode: latest`, `'reload'` is forbidden and will be rejected. 
    Use `mode: update` or `mode: append` instead if you need reconciliation behavior.


!!! tip
    You generally do not need to emit `'reload'` manually in Gold SCD update jobs; it is injected for you when `__operation` is missing. 
    For explicit control, you can produce rows with `__operation = 'reload'` at the snapshot timestamp.

---

## SCD2 details

*Behavior* (see `merge/scd2.sql.jinja`):

- Update (`__merge_condition = 'update'`): closes the current row by setting `__valid_to = __valid_from - 1 second`, `__is_current = false`. A subsequent insert creates the new current row.
- Delete (`__merge_condition = 'delete'`): closes the current row and sets `__is_current = false` (and `__is_deleted = true` if soft delete is modeled).
- Insert (`__merge_condition = 'insert'`): inserts a new current row.

*Required Gold inputs*:

- `__key`, `__timestamp`, `__operation` with values `'upsert' | 'delete' | 'reload'`.

*Reload notes*:

- `'reload'` marks a reconciliation boundary; Fabricks derives concrete actions (e.g., closing current rows, inserting new ones, deleting missing keys) across that boundary.
- If you omit `__operation` in Gold SCD update jobs, Fabricks injects `'reload'` and enables rectification automatically.
- In Silver:
    - Presence of `'reload'` (beyond target’s max timestamp) enables rectification.
    - `'reload'` is forbidden in `mode: latest`.

*Optional features*:

- `options.correct_valid_from`: adjusts start timestamps for validity windows.
- `options.persist_last_timestamp`: persists last processed timestamp for incremental loads.

*Convenience view*:

- `{table}__current`: returns only the latest (current) rows per business key.

*Minimal Silver example*:

```yaml
- job:
    step: silver
    topic: monarch
    item: scd2
    options:
      mode: update
      change_data_capture: scd2
```

*Note*: Credit — [Temporal Snapshot Fact Table (SQLBits 2012)](https://sqlbits.com/sessions/event2012/Temporal_Snapshot_Fact_Table). Recommended to watch to understand SCD2 snapshot-based modeling concepts. 

*Slides*: [Temporal Snapshot Fact Tables (slides)](https://www.slideshare.net/slideshow/temporal-snapshot-fact-tables/15900670#45)

*Gold input construction example*:

```sql
-- Turn SCD2 changes into Gold input operations
with dates as (
  select id as id, __valid_from as __timestamp, 'upsert' as __operation
  from silver.monarch_scd2 where __valid_from > '1900-01-02'
  union
  select id as id, __valid_to as __timestamp, 'delete' as __operation
  from silver.monarch_scd2 where __is_deleted
)
select
  d.id as __key,
  s.id as id,
  s.name as monarch,
  s.doubleField as value,
  d.__operation,
  if(d.__operation = 'delete', d.__timestamp + interval 1 second, d.__timestamp) as __timestamp
from dates d
left join silver.monarch_scd2 s
  on d.id = s.id and d.__timestamp between s.__valid_from and s.__valid_to
```

---

## Keys and `__key`

- Preferred: produce a stable `__key` in your SELECTs (e.g., UDF that hashes business keys).
- Alternative: configure `options.keys: [ ... ]` to specify business keys. Fabricks derives join predicates from these when `__key` is not present.

*Tip*

- Only provide `__identity` when `table_options.identity` is not true. If `identity: true`, the identity column is auto-created when the table is created; do not include `__identity`. See [Table Options](./table-options.md).

---

## Examples

Silver SCD1 with duplicates handling

```yaml
- job:
    step: silver
    topic: princess
    item: order_duplicate
    options:
      mode: update
      change_data_capture: scd1
      order_duplicate_by:
        order_by: desc
```

Gold SCD1 update with incremental timestamp

```yaml
- job:
    step: gold
    topic: scd1
    item: last_timestamp
    options:
      change_data_capture: scd1
      mode: update
      persist_last_timestamp: true
```

---

## Operational notes

- Streaming: where supported, `options.stream: true` enables incremental semantics.
- Parents: use `options.parents` to order upstream dependencies.
- Checks: configure quality gates via `options.check_options`. See [Checks & Data Quality](./checks-data-quality.md).

---

## Related

- Steps: [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Reference: [Table Options](./table-options.md), [Extenders, UDFs & Parsers](./extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md#sample-runtime)
