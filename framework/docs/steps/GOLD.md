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

Semantic table
```yaml
- job:
    step: semantic
    topic: fact
    item: job_option
    options: { mode: complete }
    table_options:
      properties:
        delta.minReaderVersion: 2
        delta.minWriterVersion: 5
```

More examples
- Example config/SQL: `framework/examples/runtime/gold/gold/_config.example.yml`, `framework/examples/runtime/gold/gold/hello_world.sql`
- Integration scenarios: `framework/tests/integration/runtime/gold/gold/` and `.../semantic/`

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
group by cp.__key, oh.customer_id, seg.segment, cp.__operation, cp.__timestamp
```

*Notes*

- The change_points CTE promotes underlying SCD2 intervals to the desired Gold grain by emitting 'upsert' at each interval start and 'delete' at interval end (or for deleted keys).
- At each change point, compute the Gold attributes as-of that timestamp. Fabricks will render the SCD2 merge using `__key`, `__timestamp`, and `__operation`.
- Article/price changes: extra unions in change_points add `silver.article_scd2` validity boundaries (within each item window) so totals recompute when article prices change.
- Joining extra tables:
  - Static/non-SCD2 tables (e.g., `silver.order_header`) can be joined directly on business keys (e.g., `order_id`).
  - SCD2 dimensions (e.g., `silver.customer_segment_scd2`) must be joined as-of the change point using a validity-window predicate: `cp.__timestamp between seg.__valid_from and seg.__valid_to`.
- If your derived attribute can also disappear (e.g., no remaining items), the 'delete' operation correctly closes the last interval for that Gold key.

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

## Related

- Next steps: [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Views](../reference/extenders-udfs-views.md)
- Sample runtime: [Sample runtime](../runtime.md#sample-runtime)
