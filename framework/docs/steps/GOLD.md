# Gold Step Reference

Gold steps produce consumption-ready models, usually via SQL. Semantic applies table properties/metadata.

## Modes

| Mode    | Description                                                                 |
|---------|-----------------------------------------------------------------------------|
| memory  | View-only result; no table is written.                                      |
| append  | Append rows to the target table.                                            |
| complete| Full refresh/overwrite of the target table.                                 |
| update  | Merge/upsert semantics (typically used with CDC).                           |
| invoke  | Run a notebook instead of SQL (configure via `invoker_options`).            |

See Gold CDC input fields below for required `__` columns when using SCD.

Quick CDC overview
- Strategies: `nocdc`, `scd1`, `scd2`
- SCD1: uses soft-delete flags `__is_current`, `__is_deleted`
- SCD2: uses validity windows `__valid_from`, `__valid_to`
- Gold inputs and operations: `__operation` values are `'upsert' | 'delete' | 'reload'`. If omitted for SCD in `mode: update`, Fabricks injects `'reload'` and enables rectification automatically

See the CDC reference for details and examples: [Change Data Capture (CDC)](../reference/cdc.md)

## Options at a glance

| Option                 | Purpose                                                                                          |
|------------------------|--------------------------------------------------------------------------------------------------|
| type                   | `default` vs `manual` (manual: you manage persistence yourself).                                 |
| mode                   | One of: `memory`, `append`, `complete`, `update`, `invoke`.                                      |
| change_data_capture    | CDC strategy: `nocdc` \| `scd1` \| `scd2`.                                                       |
| update_where           | Additional predicate limiting updates during merges.                                             |
| parents                | Explicit upstream dependencies for scheduling/recomputation.                                     |
| deduplicate            | Drop duplicate keys in the result before writing.                                                |
| persist_last_timestamp | Persist the last processed timestamp for incremental loads.                                       |
| correct_valid_from     | Adjust SCD2 timestamps that would otherwise start at a sentinel date.                            |
| table                  | Target table override (useful for semantic/table-copy scenarios).                                |
| table_options          | Delta table options and metadata (identity, clustering, properties, comments).                   |
| spark_options          | Per-job Spark session/SQL options mapping.                                                       |
| udfs                   | Path to UDFs registry to load before executing the job.                                          |
| check_options          | Configure DQ checks (pre_run, post_run, max_rows, min_rows, count_must_equal, skip).            |
| notebook               | Mark job to run as a notebook (used with `mode: invoke`).                                        |
| invoker_options        | Configure `pre_run` / `run` / `post_run` notebook execution and arguments.                       |
| requirements           | If true, install/resolve additional requirements for this job.                                   |
| timeout                | Per-job timeout seconds (overrides step defaults).                                               |

#### Option matrix (types • defaults • required)

| Option                 | Type                                        | Default | Required | Description                                                                                          |
|------------------------|---------------------------------------------|---------|----------|------------------------------------------------------------------------------------------------------|
| type                   | enum: default, manual                       | default | no       | `manual` disables auto DDL/DML; you manage persistence yourself.                                     |
| mode                   | enum: memory, append, complete, update, invoke | —     | yes      | Processing behavior.                                                                                 |
| change_data_capture    | enum: nocdc, scd1, scd2                     | nocdc   | no       | CDC strategy applied when writing (effective in `mode: update`).                                     |
| update_where           | string (SQL predicate)                      | —       | no       | Additional predicate to limit rows affected during merge/upsert.                                      |
| parents                | array[string]                               | —       | no       | Upstream dependencies to enforce scheduling/order.                                                   |
| deduplicate            | boolean                                     | false   | no       | Drop duplicate keys in the result prior to write.                                                    |
| persist_last_timestamp | boolean                                     | false   | no       | Persist last processed timestamp for incremental loads.                                              |
| correct_valid_from     | boolean                                     | —       | no       | Adjust SCD2 start timestamps when needed to avoid sentinel starts.                                   |
| table                  | string                                      | —       | no       | Target table override (useful for semantic/table-copy scenarios).                                    |
| table_options          | map                                         | —       | no       | Delta table options/metadata (identity, clustering, properties, comments).                           |
| spark_options          | map                                         | —       | no       | Per-job Spark session/SQL options.                                                                   |
| udfs                   | string (path)                               | —       | no       | Path to UDFs registry to load before executing the job.                                              |
| check_options          | map                                         | —       | no       | Data quality checks (see Checks & Data Quality).                                                     |
| notebook               | boolean                                     | —       | no       | Mark job to run as a notebook (used with `mode: invoke`).                                            |
| invoker_options        | map                                         | —       | no       | Configure `pre_run` / `run` / `post_run` notebook execution and arguments.                           |
| requirements           | boolean                                     | false   | no       | Resolve/install additional requirements for this job.                                                |
| timeout                | integer (seconds)                           | —       | no       | Per-job timeout; overrides step defaults.                                                            |

## Field reference

- *Core*
    - **type**: `default` vs `manual`. Manual means Fabricks will not auto-generate DDL/DML; you control persistence.
    - **mode**: Processing behavior (`memory`, `append`, `complete`, `update`, `invoke`).
    - **timeout**: Per-job timeout seconds; overrides step defaults.
    - **requirements**: Resolve/install additional requirements for this job.

- *CDC*
    - **change_data_capture**: `nocdc`, `scd1`, or `scd2`. Governs merge semantics when `mode: update`.

- *Incremental & merge*
    - **update_where**: Predicate that constrains rows affected during merge/upsert.
    - **deduplicate**: Drop duplicate keys prior to write.
    - **persist_last_timestamp**: Persist last processed timestamp to support incremental loads.
    - **correct_valid_from**: Adjusts start timestamps for SCD2 validity windows when needed.

- *Dependencies & targets*
    - **parents**: Explicit upstream jobs that must complete before this job runs.
    - **table**: Target table override (commonly used when coordinating with Semantic/table-copy flows).

- *Table & Spark*
    - **table_options**: Delta table options and metadata (e.g., identity, clustering, properties, comments).
    - **spark_options**: Per-job Spark session/SQL options mapping.
- *UDFs*
    - **udfs**: Path to UDFs registry to load before executing the job.
- *Checks*
    - **check_options**: Configure DQ checks (e.g., `pre_run`, `post_run`, `max_rows`, `min_rows`, `count_must_equal`, `skip`).
- *Notebook invocation*
    - **notebook**: When coupled with `mode: invoke`, mark this job to run a notebook.
    - **invoker_options**: Configure `pre_run`, `run`, and `post_run` notebooks and pass arguments.

Notes:

  - Memory outputs ignore columns starting with `__` (e.g., `__it_should_not_be_found`).

---

Minimal examples

Gold SCD1 update
```yaml
- job:
    step: gold
    topic: scd1
    item: update
    options:
      change_data_capture: scd1
      mode: update
```

Gold full refresh
```yaml
- job:
    step: gold
    topic: demo
    item: hello_world
    options:
      mode: complete
```

Scoped merge with update_where
```yaml
- job:
    step: gold
    topic: scd1
    item: scoped_update
    options:
      mode: update
      change_data_capture: scd1
      update_where: "region = 'EMEA'"   # limit merge to a subset
```


Dependencies
```sql
with cte as (select d.time, d.hour from gold.dim_time d)
select
  udf_key(array(f.id, d.time)) as __key,
  f.id as id,
  f.monarch as monarch,
  s.__source as role,
  f.value as value,
  d.time as time
from cte d
cross join transf.fact_memory f
left join silver.king_and_queen_scd1__current s on f.id = s.id
where d.hour = 10
```

Invoke (notebooks)
```yaml
- job:
    step: gold
    topic: invoke
    item: post_run
    options: { mode: memory }
    invoker_options:
      post_run:
          - notebook: gold/gold/invoke/post_run/exe
          arguments: { arg1: 1, arg2: 2, arg3: 3 }
- job:
    step: gold
    topic: invoke
    item: notebook
    options: { mode: invoke }
    invoker_options:
      run:
        notebook: gold/gold/invoke/notebook
        arguments: { arg1: 1, arg2: 2, arg3: 3 }
```

Checks and DQ
```yaml
- job:
    step: gold
    topic: check
    item: max_rows
    options: { mode: complete }
    check_options: { max_rows: 2 }
```

Check SQL contracts
```sql
-- fail.pre_run.sql
select "fail" as __action, "Please don't fail on me :(" as __message

-- warning.post_run.sql
select "I want you to warn me !" as __message, "warning" as __action
```

Additional examples
```yaml
# Fact with table options and Spark options
- job:
    step: gold
    topic: fact
    item: option
    options:
      mode: complete
    table_options:
      identity: true
      liquid_clustering: true
      cluster_by: [monarch]
      properties:
        country: Belgium
      comment: Strength lies in unity
    spark_options:
      sql:
        spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite: false
        spark.databricks.delta.properties.defaults.enableChangeDataFeed: true
```

```sql
-- order_duplicate.sql
select 1 as __key, 1 as dummy, 1 as __order_duplicate_by_desc
union all
select 1 as __key, 2 as dummy, 2 as __order_duplicate_by_desc
```

---

### Gold options

See Options at a glance and Field reference above.

### CDC input fields for gold jobs

- scd2 (required): `__key`, `__timestamp`, `__operation` ('upsert'|'delete'|'reload').
- scd1 (required): `__key`; optional `__timestamp`/`__operation` ('upsert'|'delete'|'reload') for delete/rectify handling. If `__operation` is omitted for SCD in `mode: update`, Fabricks injects `'reload'` and enables rectification.
- Optional helpers: `__order_duplicate_by_asc` / `__order_duplicate_by_desc`, `__identity`.
- See: [Change Data Capture (CDC)](../reference/cdc.md) for full details and examples.



### Gold jobs

- SCD1/SCD2 with modes and options:

```yaml
- job:
    step: gold
    topic: scd1
    item: update
    options:
      change_data_capture: scd1
      mode: update
- job:
    step: gold
    topic: scd1
    item: last_timestamp
    options:
      change_data_capture: scd1
      mode: update
      persist_last_timestamp: true
- job:
    step: gold
    topic: scd2
    item: correct_valid_from
    options:
      change_data_capture: scd2
      mode: memory
      correct_valid_from: false
```

Example SCD2 SQL using `__key`, `__timestamp`, `__operation`:

*Note*: Credit — [Temporal Snapshot Fact Table (SQLBits 2012)](https://sqlbits.com/sessions/event2012/Temporal_Snapshot_Fact_Table). Recommended to watch to understand SCD2 modeling in Gold. 


*Slides*: [Temporal Snapshot Fact Tables (slides)](https://www.slideshare.net/slideshow/temporal-snapshot-fact-tables/15900670#45)


```sql
with dates as (
  select id as id, __valid_from as __timestamp, 'upsert' as __operation from silver.monarch_scd2 where __valid_from > '1900-01-02'
  union
  select id as id, __valid_to as __timestamp, 'delete' as __operation from silver.monarch_scd2 where __is_deleted
)
select
  d.id as __key,
  s.id as id,
  s.name as monarch,
  s.doubleField as value,
  d.__operation,
  if(d.__operation = 'delete', d.__timestamp + interval 1 second, d.__timestamp) as __timestamp
from dates d
left join silver.monarch_scd2 s on d.id = s.id and d.__timestamp between s.__valid_from and s.__valid_to
```

---

# When to model SCD2 in Gold (different grain)

- Use SCD2 in Gold when the base SCD2 tables do not align with the required consumption grain, and you need to derive a slowly changing history at a coarser/different grain (e.g., roll up line-item SCD2 to order-level SCD2, or derive a customer segment SCD2 from underlying attribute histories).

Example: roll up line-item SCD2 (silver.order_item_scd2) to order-level SCD2 (total_amount by order_id)
```yaml
- job:
    step: gold
    topic: order
    item: total_amount_scd2
    options:
      mode: update
      change_data_capture: scd2
```

```sql
-- Derive Gold SCD2 change points (upserts/deletes) at the order_id grain
with change_points as (
  select order_id as __key, __valid_from as __timestamp, 'upsert' as __operation
  from silver.order_item_scd2
  union
  select order_id as __key, __valid_to as __timestamp, 'delete' as __operation
  from silver.order_item_scd2
  where __is_deleted
  union
  -- article-level price change boundaries that fall within the order_item validity window
  select oi.order_id as __key, a.__valid_from as __timestamp, 'upsert' as __operation
  from silver.order_item_scd2 oi
  join silver.article_scd2 a
    on oi.article_id = a.article_id
    and a.__valid_from between oi.__valid_from and oi.__valid_to
  union
  select oi.order_id as __key, a.__valid_to as __timestamp, 'upsert' as __operation
  from silver.order_item_scd2 oi
  join silver.article_scd2 a
    on oi.article_id = a.article_id
    and a.__valid_to between oi.__valid_from and oi.__valid_to
)
-- Compute the order-level attributes as of each change point
select
  cp.__key,
  oh.customer_id as customer_id,
  seg.segment as customer_segment,
  sum(oi.amount) as total_amount,
  cp.__operation,
  if(cp.__operation = 'delete', cp.__timestamp + interval 1 second, cp.__timestamp) as __timestamp
from change_points cp
left join silver.order_item_scd2 oi
  on cp.__key = oi.order_id
  and cp.__timestamp between oi.__valid_from and oi.__valid_to
left join silver.order_header oh
  on cp.__key = oh.order_id
left join silver.customer_segment_scd2 seg
  on oh.customer_id = seg.customer_id
  and cp.__timestamp between seg.__valid_from and seg.__valid_to
```

*Notes*

- The change_points CTE promotes underlying SCD2 intervals to the desired Gold grain by emitting 'upsert' at each interval start and 'delete' at interval end (or for deleted keys).
- At each change point, compute the Gold attributes as-of that timestamp. Fabricks will render the SCD2 merge using `__key`, `__timestamp`, and `__operation`.
- Article/price changes: extra unions in change_points add `silver.article_scd2` validity boundaries (within each item window) so totals recompute when article prices change.
- Joining extra tables:
  - Static/non-SCD2 tables (e.g., `silver.order_header`) can be joined directly on business keys (e.g., `order_id`).
  - SCD2 dimensions (e.g., `silver.customer_segment_scd2`) must be joined as-of the change point using a validity-window predicate: `cp.__timestamp between seg.__valid_from and seg.__valid_to`.
- If your derived attribute can also disappear (e.g., no remaining items), the 'delete' operation correctly closes the last interval for that Gold key.

## SCD2 with Aggregation Example (composite keys: order_id × customer_id)

This example demonstrates how to derive a Gold SCD2 stream with aggregation at a different grain than the source, by:
- Computing change points (upsert at interval starts, delete at terminal interval ends) at the target business key grain.
- Aggregating measures as of each change point over all source SCD2 segments that are valid at that timestamp.
- Zeroing out measures at delete timestamps to correctly close intervals.

```sql
with
    -- 1) Establish target business key for Gold grain (order_id × customer_id)
    source_with_business_key as (
        select
            concat_ws('*', order_id, customer_id) as composite_business_key,
            *
        from silver.order_item_scd2
    ),

    -- 2) Identify terminal segments at the target grain
    --    For terminal segments we will emit a 'delete' change point at __valid_to.
    scd2_segments as (
        select
            if(
                coalesce(
                    lead(composite_business_key) over (
                        partition by __key
                        order by __valid_from
                    ),
                    composite_business_key
                ) = composite_business_key,
                __is_deleted,
                true
            ) as is_terminal_segment,
            *
        from source_with_business_key
    ),

    -- 3) Build change points at the Gold grain:
    --    - 'upsert' at each __valid_from
    --    - 'delete' at __valid_to only for terminal segments
    change_points as (
        select
            order_id,
            customer_id,
            __valid_from as change_timestamp,
            'upsert' as change_op
        from scd2_segments

        union

        select
            order_id,
            customer_id,
            __valid_to as change_timestamp,
            'delete' as change_op
        from scd2_segments
        where is_terminal_segment
    ),

    -- 4) Aggregate measures as of each change point over active segments
    snapshot_aggregates as (
        select
            concat_ws('*', d.order_id, d.customer_id) as __key,
            d.order_id,
            d.customer_id,
            d.change_timestamp as __timestamp,
            'upsert' as __operation,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.item_qty)
            ) as total_item_qty,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.item_amount)
            ) as total_item_amount,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.item_cost)
            ) as total_item_cost,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.pending_item_qty)
            ) as pending_total_item_qty,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.pending_item_amount)
            ) as pending_total_item_amount,

            sum(
                if(d.change_op = 'delete' and d.change_timestamp = r.__valid_to, 0, r.pending_item_cost)
            ) as pending_total_item_cost
        from change_points d
        inner join scd2_segments r
            on d.change_timestamp between r.__valid_from and r.__valid_to
           and d.customer_id = r.customer_id
           and d.order_id = r.order_id
        group by d.order_id, d.customer_id, d.change_timestamp, d.change_op
    )
select *
from snapshot_aggregates;
```

Notes
- composite_business_key ensures a clear target-grain identity for reasoning about terminal segments.
- is_terminal_segment marks final intervals at the target grain, causing a 'delete' change point at __valid_to.
- At delete timestamps, measures are zeroed to close the interval cleanly and avoid double counting.
- The output conforms to Gold SCD2 inputs: (__key, __timestamp, __operation), with __operation fixed to 'upsert' as Fabricks handles rectification in update mode.
- Adjust function names (if/concat_ws) to your SQL engine equivalents if needed.

---

- Fact options: table options, clustering, properties, and Spark options:

```yaml
- job:
    step: gold
    topic: fact
    item: option
    options:
      mode: complete
    table_options:
      identity: true
      liquid_clustering: true
      cluster_by: [monarch]
      properties:
        country: Belgium
      comment: Strength lies in unity
    spark_options:
      sql:
        spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite: false
        spark.databricks.delta.properties.defaults.enableChangeDataFeed: true
```

Example SQL for this job:

```sql
select id as id, name as monarch, doubleField as value from silver.monarch_scd1__current
```

- Manual, memory, append, overwrite:

```yaml
- job: { step: gold, topic: fact, item: manual, options: { type: manual, mode: complete } }
- job: { step: gold, topic: fact, item: memory, options: { mode: memory } }
- job: { step: gold, topic: fact, item: append, options: { mode: append } }
- job: { step: gold, topic: dim, item: overwrite, options: { change_data_capture: scd1, mode: update }, table_options: { identity: true } }
```

Memory outputs ignore columns starting with `__` (e.g., `__it_should_not_be_found`).

- Deduplicate and order-by-duplicate examples:

```yaml
- job:
    step: gold
    topic: fact
    item: deduplicate
    options:
      mode: complete
      deduplicate: true
```

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

## SCD2 header-line change points with header closure (order_id × order_line_id)

This example shows how to generate SCD2 change points when combining header and line SCD2 sources, including:
- Emitting upserts at each validity start across header and lines.
- Emitting a delete for the header-only row as soon as a corresponding line exists (to close the sentinel header row).
- Emitting deletes at validity end for deleted intervals.
- Joining an additional SCD2 stream (e.g., item price) to ensure change points also reflect price boundaries.

```sql
with
  -- 1) Gather validity boundaries across header, line, and related SCD2 (price)
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

    -- ensure we have entries for the latest price window intersecting current header/line
    select h.order_id, l.order_line_id, ip.__valid_to, ip.__valid_from, false
    from silver.order_header_scd2__current h
    inner join silver.order_line_scd2__current l
      on h.order_id = l.order_id
    inner join silver.item_price_scd2 ip
      on ip.item_id = l.item_id
     and ip.price_list_id = h.price_list_id
     and ip.__is_current
  ),

  -- 2) Build change points
  change_points as (
    -- upsert at each validity start
    select order_id, order_line_id, __valid_from as __timestamp, 'upsert' as __operation
    from hdr_line_dates

    union

    -- as soon as a line exists, close the header-only row (order_line_id is NULL sentinel)
    select order_id, null as order_line_id, __valid_from as __timestamp, 'delete' as __operation
    from hdr_line_dates
    where order_line_id is not null

    union

    -- delete at validity end for deleted intervals
    select order_id, order_line_id, __valid_to as __timestamp, 'delete' as __operation
    from hdr_line_dates
    where __is_deleted
  )

-- 3) Project attributes as of each change point
select
  concat_ws('*', p.order_id, coalesce(p.order_line_id, -1)) as __key,
  p.order_id,
  p.order_line_id,

  -- header attributes (examples)
  h.customer_id,
  h.price_list_id,
  h.order_date,

  -- line attributes (examples)
  l.item_id,
  l.quantity,
  l.unit_price,

  -- CDC fields
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
- The null-sentinel header row (order_line_id = null -> coerced to -1 in __key) is closed via a 'delete' when any line appears at the same boundary.
- Additional SCD2 inputs (e.g., item_price_scd2) can be unioned into the dates set to force change points whenever related dimensions change.
- The output stream conforms to Gold SCD2 input fields: __key, __timestamp, __operation.

---

## Related

- Next steps: [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md#sample-runtime)
