# Gold Step Reference

Gold steps produce consumption-ready models, typically implemented in SQL. Gold focuses on dimensional models, facts and marts, and prepares data for analytics and reporting.

What Gold does
- Runs curated SQL transformations or notebooks to produce business-ready tables and views.
- Supports full-refresh (`complete`), append, and merge/update semantics; can also invoke notebooks for complex workflows.
- Implements CDC logic for downstream consumers when required (SCD1/SCD2 patterns).

Modes

| Mode       | Behavior |
|------------|----------|
| `memory`   | In-session view only; no persisted table. Useful for testing or transient outputs. |
| `append`   | Append-only writes to the target table. |
| `complete` | Full refresh/overwrite of the target table. Use when you need a consistent snapshot. |
| `update`   | Merge/upsert semantics for idempotent updates and CDC. |
| `invoke`   | Execute a notebook as the job body (configured via `invoker_options`). |

CDC and inputs
- Gold jobs often receive CDC-style inputs. Required CDC fields include `__key`, `__timestamp`, and `__operation` for change-point driven inputs.
- CDC strategies supported: `nocdc`, `scd1`, `scd2`. See the CDC reference for full details: [Change Data Capture (CDC)](../reference/cdc.md)

Common options (summary)

| Option | Purpose |
|--------|---------|
| `type` | `default` vs `manual`. `manual` disables Fabricks auto-DDL/DML. |
| `mode` | One of: `memory`, `append`, `complete`, `update`, `invoke`. |
| `change_data_capture` | CDC strategy: `nocdc` \| `scd1` \| `scd2`. |
| `update_where` | Predicate to limit rows affected during merge/upsert. |
| `parents` | Upstream dependencies for scheduling and recomputation. |
| `deduplicate` | Drop duplicate keys in the result before writing. |
| `persist_last_timestamp` | Persist the last processed timestamp for incremental loads. |
| `correct_valid_from` | Adjust SCD2 start timestamps for sentinel handling. |
| `table` | Target table override (useful for semantic/table-copy scenarios). |
| `table_options` | Delta table options and metadata (identity, clustering, properties, comments). |
| `spark_options` | Per-job Spark SQL/session options. |
| `udfs` | Path/registry of UDFs to load before executing the job. |
| `check_options` | Configure DQ checks (`pre_run`, `post_run`, `min_rows`, `max_rows`, etc.). |
| `notebook` / `invoker_options` | Configure notebook invocation for `mode: invoke`. |
| `requirements` | If true, install/resolve additional dependencies for this job. |
| `timeout` | Per-job timeout seconds (overrides step defaults). |

Operational guidance
- Use `type: manual` when you need explicit control over table DDL or persistence.
- When using SCD patterns, ensure your inputs include the expected `__` CDC columns.
- Use `update_where` to scope merges and avoid unintended updates.
- For heavy operations, configure `table_options` (clustering, partitioning, properties) and `spark_options` appropriately.

**Examples**

Append example
```yaml
- job:
    step: gold
    topic: fact
    item: append
    options:
      mode: `append`
```

Complete/overwrite example
```yaml
- job:
    step: gold
    topic: dim
    item: overwrite
    options:
      mode: `complete`
      change_data_capture: `scd1`
    table_options:
      identity: true
```

Deduplicate and ordering examples
```yaml
- job:
    step: gold
    topic: fact
    item: deduplicate
    options:
      mode: `complete`
      deduplicate: true
```

SQL examples (dedupe/order)
```sql
-- deduplicate.sql
select 1 as __key, 2 as dummy
union all
select 1 as __key, 1 as dummy
```

```sql
-- order_duplicate.sql
select 1 as __key, 1 as dummy, 1 as __order_duplicate_by_desc
union all
select 1 as __key, 2 as dummy, 2 as __order_duplicate_by_desc
```

SCD2 header-line change points (example)

This example shows how to generate SCD2 change points when combining header and line SCD2 sources.
- Emit upserts at each validity start across header and lines.
- Emit a delete for the header-only row as soon as a corresponding line exists (to close the sentinel header row).
- Emit deletes at validity end for deleted intervals.
- Join additional SCD2 streams (e.g., item price) to ensure change points reflect related dimension changes.

```sql
with
  hdr_line_dates as (
    select h.order_id, l.order_line_id, h.__valid_to, h.__valid_from, h.__is_deleted
    from silver.order_header_scd2 h
    left join silver.order_line_scd2 l
      on h.order_id = l.order_id
      and h.__valid_from between l.__valid_from and l.__valid_to

    union

    select order_id, order_line_id, __valid_to, __valid_from, __is_deleted
    from silver.order_line_scd2

    union

    select h.order_id, l.order_line_id, ip.__valid_to, ip.__valid_from, false
    from silver.order_header_scd2__current h
    inner join silver.order_line_scd2__current l
      on h.order_id = l.order_id
    inner join silver.item_price_scd2 ip
      on ip.item_id = l.item_id
     and ip.price_list_id = h.price_list_id
     and ip.__is_current
  ),

  change_points as (
    select order_id, order_line_id, __valid_from as __timestamp, 'upsert' as __operation
    from hdr_line_dates

    union

    select order_id, null as order_line_id, __valid_from as __timestamp, 'delete' as __operation
    from hdr_line_dates
    where order_line_id is not null

    union

    select order_id, order_line_id, __valid_to as __timestamp, 'delete' as __operation
    from hdr_line_dates
    where __is_deleted
  )

select
  concat_ws('*', p.order_id, coalesce(p.order_line_id, -1)) as __key,
  p.order_id,
  p.order_line_id,
  h.customer_id,
  h.price_list_id,
  h.order_date,
  l.item_id,
  l.quantity,
  l.unit_price,
  p.__operation,
  p.__timestamp
from change_points p
left join silver.order_header_scd2 h
  on p.order_id = h.order_id
 and p.__timestamp between h.__valid_from and h.__valid_to
left join silver.order_line_scd2 l
  on p.order_id = l.order_id
 and coalesce(p.order_line_id, -1) = coalesce(l.order_line_id, -1)
 and p.__timestamp between l.__valid_from and l.__valid_to
```

Notes
- The null-sentinel header row (order_line_id = null -> coerced to -1 in __key) is closed via a `delete` when any line appears at the same boundary.
- Additional SCD2 inputs (e.g., item_price_scd2) can be unioned into the dates set to force change points whenever related dimensions change.
- The output stream conforms to Gold SCD2 input fields: `__key`, `__timestamp`, `__operation`.

Related
- Next steps: [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Parsers](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md)
