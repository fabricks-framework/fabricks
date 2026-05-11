# Fabricks → Lakebase Migration

## Scope
Metadata and scheduling infrastructure only. Bronze/Silver/Gold **data tables stay on Delta**. Breaking change — no dual-backend toggle, Lakebase is simply the metadata backend.

## Why
- PostgreSQL MVCC handles concurrent log writes trivially vs Delta file-level locking contention
- Eliminate Azure Table Storage + Azure Queue Storage entirely
- Consolidate ~15 Delta tables + ~12 Spark views → 10 tables + 9 views

---

## Final Schema

### 10 Tables (PostgreSQL, `fabricks` schema)

**`fabricks.bronze_jobs`** / **`fabricks.silver_jobs`** / **`fabricks.gold_jobs`**
- One table per step — absorbs `*_tables` and `*_views` as columns
- Columns: `job_id TEXT PK`, `step`, `topic`, `item`, `mode`, `cdc`, `type`, `object_type` ('table'|'view'|null), `table_name`, `view_name`, `tags JSONB`, `options JSONB`, `updated_at TIMESTAMPTZ`
- Replaces: `bronze_jobs` + `bronze_tables` + `bronze_views` Delta tables (× 3 steps)

**`fabricks.runtime`**
- Replaces: `fabricks.runtime` Spark SQL view (was an unqueryable `parse_json()` blob)
- Top-level columns: `name TEXT PK`, `catalog`, `timezone`, `secret_scope`, `unity_catalog BOOLEAN`, `workers INT`, `deployed_at TIMESTAMPTZ`
- Full nested config in JSONB: `options`, `bronze`, `silver`, `gold`, `databases`, `variables`
- Written by `deploy_runtime()` via psycopg2 upsert

**`fabricks.logs`**
- Replaces: `fabricks.logs` Delta table + `AzureTableLogHandler` two-phase flush
- Logs written directly via psycopg2 INSERT during execution (no post-run flush)
- Columns: `id BIGSERIAL PK`, `schedule_id`, `schedule`, `step`, `job_id`, `job`, `notebook_id`, `level`, `status`, `timestamp TIMESTAMPTZ`, `exception JSONB` (was StructType), `json JSONB` (was VariantType)

**`fabricks.dependencies`**
- Replaces: `bronze/silver/gold_dependencies` Delta tables + `fabricks.dependencies` union view
- Columns: `dependency_id`, `job_id`, `parent_id`, `parent`, `origin`, `step NOT NULL`, PK on `(job_id, parent_id)`

**`fabricks.job_state`**
- Replaces THREE mechanisms:
  1. Delta TBLPROPERTIES `fabricks.last_version` / `fabricks.last_batch` (via `table.get_property()` / `table.set_property()`)
  2. Per-Gold-job `{item}__last_timestamp` Delta table (N separate tables, one per job with `persist_last_timestamp: true`)
  3. `_persist_timestamp()` + `cdc_last_timestamp` branching in `gold.py`
- Columns: `job_id TEXT PK`, `step`, `topic`, `item`, `last_version BIGINT`, `last_batch TEXT`, `last_timestamp TIMESTAMPTZ`, `last_valid_from TIMESTAMPTZ`, `last_updated TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`
- Enables: centralized "where is each job up to?" query — previously impossible without reading Delta metadata table-by-table

**`fabricks.schedules`**
- New — no current equivalent
- Columns: `schedule_id TEXT PK`, `schedule`, `started_at TIMESTAMPTZ`, `finished_at TIMESTAMPTZ`, `status` ('running'|'done'|'failed')

**`fabricks.schedule_jobs`**
- Replaces: Azure Table Storage `statuses` partition (temp per-run table named `t{schedule_id}`)
- Columns: `schedule_id`, `job_id`, `job`, `step`, `rank INTEGER`, `status` ('scheduled'|'waiting'|'starting'|'ok'), PK on `(schedule_id, job_id)`
- Lives on the **schedule branch**, not main — deleted when branch is deleted at `terminate()`

**`fabricks.schedule_dependencies`**
- Replaces: Azure Table Storage `dependencies` partition
- Columns: `schedule_id`, `job_id`, `parent_id`, PK on `(schedule_id, job_id, parent_id)`
- Lives on the **schedule branch**, not main — deleted when branch is deleted at `terminate()`

---

### 9 Views (PostgreSQL views, not Spark SQL)

| View | Notes |
|---|---|
| `fabricks.jobs` | `UNION ALL` of `bronze_jobs`, `silver_jobs`, `gold_jobs` |
| `fabricks.logs_pivot` | aggregate logs per job/schedule; `collect_set` → `array_agg(DISTINCT)`, `date_diff` → `EXTRACT(EPOCH FROM ...)` |
| `fabricks.last_schedule` | latest `schedule_id` from `logs_pivot` |
| `fabricks.last_status` | latest run per `job_id`; `QUALIFY ROW_NUMBER()` → CTE subquery |
| `fabricks.previous_schedule` | second-to-last schedule |
| `fabricks.schedules_summary` | stats per schedule (duration, failed, done counts) |
| `fabricks.dependencies_recursive` | replaces 10-level self-join + UNPIVOT with `WITH RECURSIVE` CTE |
| `fabricks.dependencies_circular` | circular dependency detection |
| `fabricks.jobs_to_be_updated` | object_type drift; simplified because `table_name`/`view_name` are now columns on `*_jobs` |

---

## What is Fully Eliminated

### Azure Storage (both SDKs removed)
- `AzureTable` (`azure.data.tables`) — temp per-schedule job coordination
- `AzureQueue` (`azure.storage.queue`) — job dispatch; replaced by `SELECT FOR UPDATE SKIP LOCKED` on `schedule_jobs`
- `AzureTableLogHandler` — Python logging handler writing to Azure Table; logs now go direct to `fabricks.logs`
- `write_logs()` in `base.py` — post-run flush from Azure Table → Delta; no longer needed
- `get_connection_info()` / `_get_access_key_from_secret_scope()` in `dags/utils.py`
- `azure-data-tables` and `azure-storage-queue` pip dependencies

### Delta metadata tables
- `fabricks.steps` — derived from runtime YAML, no longer a table
- `fabricks.dummy` — streaming init hack, handled internally
- `bronze/silver/gold_tables` (6 tables) — absorbed into `*_jobs.table_name`
- `bronze/silver/gold_views` (6 tables) — absorbed into `*_jobs.view_name`
- `bronze/silver/gold_dependencies` (3 tables) — consolidated into `fabricks.dependencies`
- `gold.{topic}.{item}__last_timestamp` (N tables) — consolidated into `fabricks.job_state`

### Spark SQL views (replaced by PG views)
- All views in `fabricks/deploy/views.py`
- `fabricks.runtime` view in `fabricks/deploy/runtime.py`

---

## Key Design Decisions

- **No partitioning for write performance** — PostgreSQL MVCC handles concurrent INSERTs natively; partitioning only helps query pruning and archival, not write contention
- **Azure Queue replaced by `SELECT FOR UPDATE SKIP LOCKED`** — standard PG-native job queue pattern; multiple worker threads query concurrently, PG guarantees each gets a different row atomically
- **`job_state` is the big simplification win** — 3 separate mechanisms → 1 table, and enables a query that was previously impossible
- **`runtime` promoted to a real table** — queryable and joinable, not a JSON blob embedded in a view
- **`*_jobs` absorbs `*_tables` + `*_views`** — `object_type`, `table_name`, `view_name` become columns, eliminating 6 separate Delta tables
- **Logs are written once, directly** — eliminates the two-phase Azure Table → Delta flush
- **Schedule branching** — each active schedule runs on its own Lakebase branch (see below)

---

## Schedule Branching

Each schedule run uses a **Lakebase database branch** for full isolation of in-flight state.

### Lifecycle

```
main (always clean)
    │
    ├── generate() → create branch: schedule_{schedule_id}
    │       │
    │       ├── schedule_jobs written on branch
    │       ├── schedule_dependencies written on branch
    │       ├── workers SELECT FOR UPDATE SKIP LOCKED on branch
    │       └── logs + job_state updates written on branch
    │
    └── terminate() → merge logs → main fabricks.logs
                    → merge job_state → main fabricks.job_state
                    → delete branch (drops schedule_jobs + schedule_dependencies atomically)
```

### Why branching

- **No explicit cleanup** — branch deletion removes `schedule_jobs` + `schedule_dependencies` + in-flight state atomically
- **True isolation** — a crashing schedule cannot leave partial state in main; just delete the branch
- **Parallel schedules** — each gets its own branch, zero contention on coordination tables
- **Free rollback** — if a schedule fails badly, discard the branch; main is untouched

### Real-time observability trade-off

Logs and `job_state` are only visible on main after `terminate()`. If live progress queries during execution are needed, write logs to **both** branch and main — branch for isolation, main for observability. Coordination tables (`schedule_jobs`, `schedule_dependencies`) stay branch-only regardless.

### Connection management

Branch has its own connection string. `DagGenerator` creates the branch and passes the endpoint to `DagProcessor` workers:

```python
# generator.py
branch_conn_str = lakebase.create_branch(f"schedule_{schedule_id}")
# passed to all DagProcessor workers via task values / widgets
# workers build their ThreadedConnectionPool against branch_conn_str, not main
```

### New file needed
- `fabricks/metastore/lakebase/branch.py` — `create_branch(name)`, `merge_branch(name, tables)`, `delete_branch(name)` wrapping the Lakebase branching API

## Config Change (Breaking)

Add to `RuntimeOptions` — required, no default:
```python
lakebase_host: str      # Lakebase instance hostname
lakebase_secret: str    # secret key name within existing secret_scope
```

---

## Files to Change

### New
- `fabricks/metastore/lakebase/` — connection pool, credentials, `LakebaseTable`, `LakebaseDatabase`, `branch.py`
- `fabricks/deploy/lakebase_tables.py` — DDL for all `fabricks.*` tables
- `fabricks/deploy/lakebase_views.py` — all PG view definitions

### Modified
- `fabricks/models/runtime/models.py` — add `lakebase_host`, `lakebase_secret`
- `fabricks/deploy/tables.py` — replace Delta table creation with psycopg2
- `fabricks/deploy/views.py` — replace Spark SQL with PG views
- `fabricks/deploy/runtime.py` — replace `SPARK.sql(CREATE OR REPLACE VIEW ...)` with psycopg2 upsert
- `fabricks/core/dags/base.py` — remove Azure imports, remove `write_logs()`
- `fabricks/core/dags/processor.py` — replace Azure Table/Queue with PG `schedule_jobs` + `SELECT FOR UPDATE SKIP LOCKED`
- `fabricks/core/dags/generator.py` — replace `AzureQueue` with inserts into `schedule_jobs` / `schedule_dependencies`
- `fabricks/utils/log.py` — replace `AzureTableLogHandler` with psycopg2 log handler
- `fabricks/core/jobs/base/processor.py` — replace `get_property`/`set_property` with `job_state` repo
- `fabricks/core/jobs/base/generator.py` — remove `fabricks.last_version` default TBLPROPERTY
- `fabricks/core/jobs/gold.py` — remove `cdc_last_timestamp`, `_persist_timestamp`, `persist_last_timestamp` branching

### Deleted
- `fabricks/utils/azure_table.py`
- `fabricks/utils/azure_queue.py`
- `fabricks/core/dags/utils.py` (Azure credential helpers)
