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
- Written by `deploy_runtime()` via psycopg2 upsert to **main**

**`fabricks.logs`**
- Replaces: `fabricks.logs` Delta table + `AzureTableLogHandler` two-phase flush
- Written to **branch** during execution; merged to main at `terminate()`
- If live observability is needed: write to both branch and main during execution (open decision)
- Columns: `id BIGSERIAL PK`, `schedule_id`, `schedule`, `step`, `job_id`, `job`, `notebook_id`, `level`, `status`, `timestamp TIMESTAMPTZ`, `exception JSONB` (was StructType), `json JSONB` (was VariantType)

**`fabricks.dependencies`**
- Replaces: `bronze/silver/gold_dependencies` Delta tables + `fabricks.dependencies` union view
- Columns: `dependency_id`, `job_id`, `parent_id`, `parent`, `origin`, `step NOT NULL`, PK on `(job_id, parent_id)`
- Written to **main** at deploy time (static, not per-run)

**`fabricks.job_state`**
- Replaces THREE mechanisms:
  1. Delta TBLPROPERTIES `fabricks.last_version` / `fabricks.last_batch` (via `table.get_property()` / `table.set_property()`)
  2. Per-Gold-job `{item}__last_timestamp` Delta table (N separate tables, one per job with `persist_last_timestamp: true`)
  3. `_persist_timestamp()` + `cdc_last_timestamp` branching in `gold.py`
- Columns: `job_id TEXT PK`, `step`, `topic`, `item`, `last_version BIGINT`, `last_batch TEXT`, `last_timestamp TIMESTAMPTZ`, `last_valid_from TIMESTAMPTZ`, `last_updated TIMESTAMPTZ`, `updated_at TIMESTAMPTZ`
- Updated on **branch** during execution; merged to main at `terminate()`
- Enables: centralized "where is each job up to?" query — previously impossible without reading Delta metadata table-by-table

**`fabricks.schedules`**
- Written to **main** at `generate()` (status='running') and updated at `terminate()` (status='done'|'failed')
- Must be on main (not branch) so the running schedule is immediately visible
- Columns: `schedule_id TEXT PK`, `schedule`, `started_at TIMESTAMPTZ`, `finished_at TIMESTAMPTZ`, `status` ('running'|'done'|'failed')

**`fabricks.schedule_jobs`**
- Replaces: Azure Table Storage `statuses` partition (temp per-run table named `t{schedule_id}`)
- Columns: `schedule_id`, `job_id`, `job`, `step`, `rank INTEGER`, `status` ('scheduled'|'waiting'|'starting'|'ok'), PK on `(schedule_id, job_id)`
- Lives on **branch** only — pre-exists as empty table on main so it's present on the branch at creation time; deleted when branch is deleted at `terminate()`

**`fabricks.schedule_dependencies`**
- Replaces: Azure Table Storage `dependencies` partition
- Columns: `schedule_id`, `job_id`, `parent_id`, PK on `(schedule_id, job_id, parent_id)`
- Lives on **branch** only — pre-exists as empty table on main; deleted when branch is deleted at `terminate()`

---

### 9 Views (PostgreSQL views on main, not Spark SQL)

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
- `AzureTableLogHandler` — Python logging handler writing to Azure Table; replaced by psycopg2 log handler writing to branch
- `write_logs()` in `base.py` — post-run flush from Azure Table → Delta; replaced by branch merge at `terminate()`
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
- **`job_state` is the big simplification win** — 3 separate mechanisms → 1 table, enables a query that was previously impossible
- **`runtime` promoted to a real table** — queryable and joinable, not a JSON blob embedded in a view
- **`*_jobs` absorbs `*_tables` + `*_views`** — `object_type`, `table_name`, `view_name` become columns, eliminating 6 separate Delta tables
- **Schedule branching** — each active schedule runs on its own Lakebase branch for full isolation (see below)
- **`fabricks.schedules` written to main** — the persistent run record must be visible immediately, not after branch merge

---

## Schedule Branching

Each schedule run uses a **Lakebase database branch** for full isolation of in-flight state.

### Lifecycle

```
main (always clean)
    │
    ├── generate()
    │     ├── INSERT INTO fabricks.schedules (status='running')  → main
    │     ├── create branch: {schedule_id}
    │     │     (schedule_jobs + schedule_dependencies pre-exist as empty tables on main,
    │     │      so they are present on the branch at creation time)
    │     └── pass branch connection string to DagProcessor workers via Databricks task values
    │
    ├── execution (all workers connect to branch)
    │     ├── INSERT INTO schedule_jobs           → branch
    │     ├── INSERT INTO schedule_dependencies   → branch
    │     ├── SELECT FOR UPDATE SKIP LOCKED       → branch
    │     ├── INSERT INTO fabricks.logs           → branch (+ main if live observability needed)
    │     └── UPSERT INTO fabricks.job_state      → branch
    │
    └── terminate()  (DagTerminator)
          ├── merge fabricks.logs       branch → main
          ├── merge fabricks.job_state  branch → main
          ├── UPDATE fabricks.schedules SET status='done'|'failed', finished_at=now()  → main
          └── delete branch  (atomically removes schedule_jobs + schedule_dependencies)
```

### Why branching

- **No explicit cleanup** — branch deletion removes `schedule_jobs` + `schedule_dependencies` + in-flight state atomically
- **True isolation** — a crashing schedule cannot leave partial state in main; just delete the branch
- **Parallel schedules** — each gets its own branch, zero contention on coordination tables
- **Free rollback** — if a schedule fails badly, discard the branch; main is untouched

### Open decision: log observability during execution

| Option | Behaviour | Trade-off |
|---|---|---|
| Branch-only | Logs visible on main only after `terminate()` | Simplest; no live progress visible |
| Dual-write | Logs go to branch AND main during execution | Live observability; one extra INSERT per log entry |

Coordination tables (`schedule_jobs`, `schedule_dependencies`) stay branch-only regardless.

### Connection management

Branch has its own connection string. `DagGenerator` creates the branch and passes the endpoint to `DagProcessor` workers via Databricks task values:

```python
# generator.py
branch_conn_str = lakebase.create_branch(f"{schedule_id}")
dbutils.jobs.taskValues.set(key="branch_conn_str", value=branch_conn_str)
# workers retrieve it and build their ThreadedConnectionPool against it
```

---

## Data Flow: YAML → Lakebase

### Current path (YAML → Delta)

```
YAML _config.*.yml
  → read_yaml()              dict iterator       pure Python
  → SPARK.createDataFrame()  Spark DataFrame     only needed because Delta requires Spark
  → NoCDC("fabricks", ...)   CDC merge           Delta write
  → fabricks.bronze_jobs     Delta table
```

Spark is in the middle only because Delta requires it. With PostgreSQL it is unnecessary overhead for metadata writes.

### New path (YAML → Lakebase)

```
YAML _config.*.yml
  → read_yaml()              dict iterator       unchanged
  → JobConf.model_validate() Pydantic model      already happens today
  → model.model_dump()       plain dict          no Spark involved
  → execute_values()         psycopg2 batch      INSERT ... ON CONFLICT DO UPDATE
  → fabricks.bronze_jobs     PG table
```

Same pattern applies to `dependencies`, `runtime`, and `job_state` — all are Python-native data that only went through Spark because Delta required it.

### Data path per table

| Table | Source | Write mechanism |
|---|---|---|
| `bronze/silver/gold_jobs` | YAML `_config.*.yml` via `read_yaml()` | `model_dump()` → `execute_values()` |
| `dependencies` | Python computation in `BaseStep._get_dependencies_internal()` | dict → `execute_values()` |
| `runtime` | `CONF_RUNTIME.model_dump()` in `deploy_runtime()` | psycopg2 upsert |
| `logs` | Python `logging` records during execution | psycopg2 log handler → `INSERT` to branch |
| `job_state` | post-run in `processor.py` / `gold.py` | psycopg2 upsert after each job completes |
| `schedules` | `DagGenerator.generate()` / `DagTerminator.terminate()` | psycopg2 INSERT / UPDATE to main |
| `schedule_jobs` | `DagGenerator` query result | psycopg2 `execute_values()` to branch |
| `schedule_dependencies` | `DagGenerator` dependency resolution | psycopg2 `execute_values()` to branch |

### The tricky part: `DagGenerator.get_jobs()`

This currently runs a Spark SQL query joining three sources:

```sql
SELECT ... FROM fabricks.jobs j                    -- will be a PG view
INNER JOIN fabricks.{schedule}_schedule v ON ...   -- currently a Spark SQL view
LEFT JOIN fabricks.logs_pivot l ON ...             -- will be a PG view
```

With `fabricks.jobs` and `fabricks.logs_pivot` moving to PG, Spark cannot see them natively. Two options:

| Option | How | Trade-off |
|---|---|---|
| **JDBC** | Read PG tables into Spark via JDBC, join with Spark schedule view | Keeps `{schedule}_schedule` views in Spark; JDBC overhead |
| **Move schedule views to PG** | Recreate `{schedule}_schedule` as PG views; run full query via psycopg2 | Cleaner; eliminates Spark from scheduling path entirely |

**Preferred: move schedule views to PG.** The `{schedule}_schedule` views filter `fabricks.jobs` by schedule — trivial to port. This removes Spark from the entire schedule generation and coordination path.

### `update_configurations()` in `BaseStep`

Currently calls `NoCDC("fabricks", self.name, "jobs").delete_missing(df, keys=["job_id"])` which writes a Spark DataFrame to Delta. Becomes:

```python
def update_configurations(self, drop=False):
    jobs = list(self.get_jobs_iter())
    repo.upsert_many("fabricks.bronze_jobs", jobs)
    repo.delete_missing("fabricks.bronze_jobs", job_ids=[j["job_id"] for j in jobs])
```

`get_jobs()` returning a Spark DataFrame is no longer needed for the metadata write path. Call sites that still use it for Spark-side filtering need auditing.

### `update_tables_list()` and `update_views_list()` in `BaseStep`

Currently write to `fabricks.bronze_tables` / `fabricks.bronze_views` Delta tables via `NoCDC`. These tables are eliminated — `table_name`, `view_name`, `object_type` are now columns on `bronze_jobs`. These methods become a single:

```sql
UPDATE fabricks.bronze_jobs SET table_name=?, view_name=?, object_type=? WHERE job_id=?
```

### `update_steps_list()` in `BaseStep`

Currently writes to `fabricks.steps` Delta table via `NoCDC`. Table eliminated — steps derived from runtime YAML. Method deleted entirely.

### Passing data to jobs during execution

There are two separate concerns: job config and schedule context.

**Job config — `get_job_conf.py`**

Already has two paths via `IS_JOB_CONFIG_FROM_YAML`:

```python
if IS_JOB_CONFIG_FROM_YAML:
    # reads YAML files directly — unchanged, works as-is
    iter = s.get_jobs_iter()
    conf = next(i for i in iter if ...)
else:
    # currently Spark SQL → Delta; becomes psycopg2 → PG
    row = repo.get_one(
        f"SELECT * FROM fabricks.{step}_jobs WHERE job_id = %s", (job_id,)
    )
    return get_job_conf_internal(step=step, row=row)
```

The `IS_JOB_CONFIG_FROM_YAML = True` path is untouched.

**Schedule context — `run.py` and `DagProcessor.receive()`**

Currently `schedule_id` and `schedule` are passed as notebook arguments and retrieved via Databricks task values or widgets. `branch_conn_str` follows the exact same pattern — added as a new notebook argument by `DagProcessor.receive()`:

```python
arguments={
    "schedule_id": self.schedule_id,
    "schedule": self.schedule,
    "step": str(self.step),
    "job_id": j.get("JobId"),
    "job": j.get("Job"),
    "branch_conn_str": self.branch_conn_str,   # new
}
```

Retrieved in `run.py` via:
```python
branch_conn_str = dbutils.jobs.taskValues.get(taskKey="initialize", key="branch_conn_str")
```

Used to build the psycopg2 branch connection for log writes and `job_state` updates during that job's execution.

**Main vs branch connection**

| Connection | Purpose | How initialized |
|---|---|---|
| **Main** | read job config, read `runtime`, final merge at `terminate()` | module-level constant in `fabricks/context/` from `RuntimeOptions.lakebase_host` + `lakebase_secret` at import time |
| **Branch** | write `logs`, write `job_state` during run, read/write `schedule_jobs` | passed as `branch_conn_str` notebook argument per schedule run |

---

## Config Change (Breaking)

Add to `RuntimeOptions` — required, no default:
```python
lakebase_host: str      # Lakebase instance hostname
lakebase_secret: str    # secret key name within existing secret_scope
```

---

## Files to Change

### New
- `fabricks/metastore/lakebase/` — connection pool, credentials, `LakebaseTable`, `LakebaseDatabase`
- `fabricks/metastore/lakebase/branch.py` — `create_branch(name)`, `merge_branch(name, tables)`, `delete_branch(name)` wrapping the Lakebase branching API
- `fabricks/deploy/lakebase_tables.py` — DDL for all `fabricks.*` tables
- `fabricks/deploy/lakebase_views.py` — all PG view definitions

### Modified
- `fabricks/models/runtime/models.py` — add `lakebase_host`, `lakebase_secret`
- `fabricks/deploy/tables.py` — replace Delta table creation with psycopg2
- `fabricks/deploy/views.py` — replace Spark SQL with PG views
- `fabricks/deploy/runtime.py` — replace `SPARK.sql(CREATE OR REPLACE VIEW ...)` with psycopg2 upsert
- `fabricks/core/dags/base.py` — remove Azure imports, remove `write_logs()`
- `fabricks/core/dags/generator.py` — create Lakebase branch, pass branch conn string via task values; replace `AzureQueue` with inserts into `schedule_jobs` / `schedule_dependencies` on branch
- `fabricks/core/dags/processor.py` — replace Azure Table/Queue with PG `schedule_jobs` + `SELECT FOR UPDATE SKIP LOCKED` on branch
- `fabricks/core/dags/terminator.py` — merge branch logs + job_state → main, update `fabricks.schedules`, delete branch
- `fabricks/utils/log.py` — replace `AzureTableLogHandler` with psycopg2 log handler writing to branch
- `fabricks/core/jobs/base/processor.py` — replace `get_property`/`set_property` with `job_state` repo
- `fabricks/core/jobs/base/generator.py` — remove `fabricks.last_version` default TBLPROPERTY
- `fabricks/core/jobs/gold.py` — remove `cdc_last_timestamp`, `_persist_timestamp`, `persist_last_timestamp` branching
- `fabricks/core/steps/base.py` — `update_configurations()` → psycopg2 upsert; `update_tables_list()` / `update_views_list()` → single UPDATE on `*_jobs`; delete `update_steps_list()`
- `fabricks/core/schedules/views.py` — recreate `{schedule}_schedule` Spark views as PG views so `DagGenerator` can query them via psycopg2
- `fabricks/core/jobs/get_job_conf.py` — non-YAML path: replace `SPARK.sql(f"select * from fabricks.{step}_jobs")` with psycopg2 `SELECT` on main connection
- `fabricks/core/dags/run.py` — retrieve `branch_conn_str` from task values/widgets; replace `TABLE_LOG_HANDLER` with psycopg2 branch log handler

### Deleted
- `fabricks/utils/azure_table.py`
- `fabricks/utils/azure_queue.py`
- `fabricks/core/dags/utils.py` (Azure credential helpers)
