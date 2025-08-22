### Gold Step Reference 

Gold steps produce consumption-ready models, usually via SQL. Semantic applies table properties/metadata.

Modes
- memory: View only
- append: Append to table
- complete: Full refresh/overwrite
- update: Merge/upsert
- invoke: Run a notebook instead of SQL (configured via invoker_options)

CDC input fields (when using SCD)
- scd2 (required): `__key`, `__timestamp`, `__operation` ('upsert'|'delete')
- scd1 (required): `__key`; optional `__timestamp`/`__operation` for deletes
- Optional helpers: `__order_duplicate_by_asc` / `__order_duplicate_by_desc`, `__identity` (with identity tables)

Common options
- type: default vs manual
- parents, update_where, deduplicate
- correct_valid_from, persist_last_timestamp
- table: target override
- timeout, requirements
- invoker_options: pre_run/run/post_run notebook execution

Semantic (grouped with gold)
- Mode: complete
- Applies table options/properties and can copy from `options.table`

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

SCD2 in gold (pattern)
```sql
with
  newkey as (
    select only_offer_id as __key, * except(__key) from offer_and_lines_table d
  ),
  deletes as (
    select only_offer_id as __key, max(__valid_to) + interval 1 second as deleted_date
    from offer_and_lines_table
    group by only_offer_id
    having max(`__valid_to`) < '9999-12-31'
  ),
  dates as (
    select __key, __valid_from as __timestamp, 'upsert' as __operation from newkey
    union
    select __key, deleted_date as __timestamp, 'delete' as __operation from deletes
  )
select
  d.__key,
  d.__timestamp,
  d.__operation,
  sum(case when d.__operation = 'delete' and d.__timestamp = r.__valid_to then 0 else sales end) as sales,
  sum(case when d.__operation = 'delete' and d.__timestamp = r.__valid_to then 0 else sales_gross end) as sales_gross
from dates d
left join newkey r on d.__timestamp between r.__valid_from and r.__valid_to and d.only_offer_id = r.only_offer_id
group by d.only_offer_id, __timestamp, __operation
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


### Gold options

- type: Default vs manual (manual: you manage persistence yourself).
- mode:
  - memory: View-only result.
  - append: Append rows to table.
  - complete: Full overwrite of the table.
  - update: Merge/update into table.
  - invoke: Run a notebook instead of SQL (see invoker_options).
- change_data_capture: `nocdc` | `scd1` | `scd2` (same semantics as silver).
- update_where: Additional predicate limiting updates.
- parents: Explicit dependencies for scheduling and recomputation.
- deduplicate: Drop duplicate keys before write.
- correct_valid_from: For SCD2 pipelines, adjust timestamps that would otherwise start at a sentinel date.
- persist_last_timestamp: Persist the last processed timestamp for incremental loads.
- table: Target table override (mainly used in semantic or table-copy jobs).
- notebook: Mark job to run as notebook (used with `mode: invoke`).
- requirements: If true, install/resolve extra requirements for this job.
- timeout: Per-job timeout seconds.

### CDC input fields for gold jobs

- scd2 (required): `__key`, `__timestamp`, `__operation` ('upsert'|'delete').
- scd1 (required): `__key`; optional `__timestamp`/`__operation` for delete handling.
- Optional helpers: `__order_duplicate_by_asc` / `__order_duplicate_by_desc`, `__identity` (with `table_options.identity: true`).

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

### Gold CDC input fields (`__` columns)

- scd2 (required):
  - `__key`: Unique identifier for the row version (any type). Often built with a function like `udf_key(...)`.
  - `__timestamp`: Effective time of the change/delete (builds validity windows).
  - `__operation`: Either `'upsert'` or `'delete'`.

- scd1:
  - `__key`: Unique identifier for the entity to upsert.
  - Optional but recommended when handling deletes:
    - `__timestamp` and `__operation` to correctly mark deletions.

- Optional helpers (both scd1/scd2):
  - `__order_duplicate_by_asc` / `__order_duplicate_by_desc`: When multiple candidate rows share the same `__key`, pick the one with the highest/lowest order value accordingly.
  - `__identity`: When `table_options.identity: true`, provide a deterministic identity value to persist.
