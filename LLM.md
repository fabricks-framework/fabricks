# Fabricks LLM Guide

This guide helps LLMs understand Fabricks and assist users with setup, configuration, steps, and schedules.

## What is Fabricks?

**Fabricks** (Framework for Databricks) is a Python framework for building Lakehouse pipelines in Databricks. It uses:
- **YAML configuration** for orchestration (jobs, steps, schedules)
- **SQL-first** transformations (99% of business logic)
- **Tiered architecture**: Bronze → Silver → Gold layers
- **Built-in CDC** (Change Data Capture) patterns (SCD1/SCD2)
- **Data quality gates** via checks and contracts

### Key Concepts

- **Step**: A processing layer/type (Bronze, Silver, Gold) that defines behavior and defaults
- **Job**: A concrete unit of work within a step, identified by `topic` + `item` (e.g., `bronze.sales_data.daily_transactions`)
- **Schedule**: Groups jobs and defines execution order across steps
- **Runtime**: The folder containing your YAML configs, SQL files, and job definitions

---

## Setup and Initialization

### 1. Installation

Fabricks is installed on Databricks clusters via PyPI:
- Libraries → Install New → PyPI → `fabricks`
- Or install from source: `pip install -e .[dev,test]`

### 2. Configuration

Fabricks discovers its runtime via configuration files or environment variables. Configuration is resolved in the following order (first found wins):

1. **`fabricksconfig.json`** (checked first, walking up directory tree)
2. **`pyproject.toml`** (checked if `fabricksconfig.json` not found)
3. **Environment variables** (used if no config files found)

**Option A: fabricksconfig.json**

```json
{
    "runtime": "path/to/your/runtime",
    "notebooks": "fabricks/api/notebooks",
    "job_config_from_yaml": true,
    "loglevel": "info",
    "debugmode": false,
    "devmode": false,
    "funmode": false,
    "config": "path/to/runtime/fabricks/conf.fabricks.yml"
}
```

**Option B: pyproject.toml**

```toml
[tool.fabricks]
runtime = "path/to/your/runtime"
notebooks = "fabricks/api/notebooks"
job_config_from_yaml = true
loglevel = "info"
debugmode = false
config = "path/to/runtime/fabricks/conf.fabricks.yml"
```

**Option C: Environment Variables**

```
FABRICKS_RUNTIME=/Workspace/Repos/your/repo/runtime
FABRICKS_CONFIG=/Workspace/Repos/your/repo/runtime/fabricks/conf.fabricks.yml
FABRICKS_NOTEBOOKS=/Workspace/Repos/your/repo/notebooks
FABRICKS_LOGLEVEL=INFO
FABRICKS_IS_JOB_CONFIG_FROM_YAML=true
FABRICKS_IS_DEBUGMODE=false
FABRICKS_IS_DEVMODE=false
FABRICKS_IS_FUNMODE=false
```

### 3. Runtime Structure

```
runtime/
  fabricks/
    conf.fabricks.yml          # Main runtime configuration
    schedules/
      schedule.yml             # Schedule definitions
    views/                     # Optional: SQL views for job filtering
    udfs/                      # Optional: User-defined functions
    extenders/                 # Optional: Python extenders
    parsers/                   # Optional: Custom parsers
  bronze/
    _config.example.yml
  silver/
    _config.example.yml        # No SQL files - reads from parents
  gold/
    gold/
      _config.example.yml
      *.sql                    # Required SQL transformation files
```

### 4. Initialization (armageddon)

Run once to bootstrap metadata, databases, and views:

```python
from fabricks.core.scripts.armageddon import armageddon

armageddon()                                      # All steps
armageddon(steps=["bronze", "silver", "gold"])    # Specific steps
```

---

## Deploy Class

```python
from fabricks.deploy import Deploy
```

### Methods

#### `Deploy.tables(drop: bool = False)`

Creates Fabricks system tables in the `fabricks` database:
- `fabricks.logs` — execution logs
- `fabricks.steps` — step metadata
- `fabricks.dummy` — used for `count_must_equal` checks

#### `Deploy.views()`

Creates system views in `fabricks` database: `jobs`, `tables`, `views`, `dependencies`, `dependencies_flat`, `dependencies_circular`, `last_schedule`, `last_status`, `schedules`, `logs_pivot`.

#### `Deploy.udfs(override: bool = True)`

Deploys UDFs from `fabricks/udfs/` and registers `fabricks.udf_job_id()`.

#### `Deploy.masks(override: bool = True)`

Deploys data masking functions from `fabricks/masks/`.

#### `Deploy.notebooks()`

Copies helper notebooks from the Fabricks package to the configured `notebooks` path: `cluster.py`, `initialize.py`, `process.py`, `schedule.py`, `run.py`, `terminate.py`.

#### `Deploy.schedules()`

Creates `fabricks.{schedule_name}_schedule` views and deploys custom views from `fabricks/views/`.

#### `Deploy.variables()`

Deploys runtime variables from the configured variables source.

#### `Deploy.runtime()`

Deploys the runtime configuration.

#### `Deploy.step(step: str)`

Convenience shortcut: runs `Deploy.tables()`, creates the step (`get_step(step).create()`), then runs `Deploy.views()` and `Deploy.schedules()`.

#### `Deploy.job(step: str)`

Convenience shortcut: creates all jobs in the step (`get_step(step).create()`).

#### `Deploy.armageddon(steps=None, nowait: bool = False)`

**Destructive full reset** — drops and recreates everything:
1. Drops `fabricks` database, all step databases, and temp storage folders
2. Recreates `fabricks` DB, system tables, UDFs, masks, notebooks
3. Runs `BaseStep.create()` for each step (creates DB, reads YAML configs, creates tables/views, updates dependencies)
4. Deploys views and schedules

`steps` accepts `None` (all), `"gold"`, or `["bronze", "silver", "gold"]`.

### Deployment Workflows

**Initial setup:**
```python
Deploy.armageddon(steps=None)
```

**Update individual components after changes:**
```python
Deploy.udfs()       # After adding UDFs
Deploy.views()      # After adding views
Deploy.schedules()  # After updating schedules
```

**Add/update a single job (preferred — much faster than step.update()):**
```python
from fabricks.api import get_job
job = get_job(step="gold", topic="sales", item="new_report")
job.create()
```

**Bulk update all jobs in a step:**
```python
from fabricks.api import get_step
step = get_step("gold")
step.update()  # Processes ALL jobs — can take significant time
```

---

## Main API Functions

```python
from fabricks.api import get_job, get_jobs, get_step
```

**Performance note**: Prefer `get_job()` over `get_step()` for individual operations. Step-level methods process every job in the step and can take minutes to hours.

### get_job()

```python
def get_job(
    job: Optional[str] = None,         # "gold.sales_monthly_summary"
    step: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    job_id: Optional[str] = None,
    row: Optional[Row] = None,
) -> BaseJob
```

```python
job = get_job(job="gold.sales_monthly_summary")
job = get_job(step="gold", topic="sales", item="monthly_summary")
job = get_job(step="gold", job_id="abc123...")
job = get_job(row=spark.sql("select * from fabricks.jobs where step='gold'").first())
```

### get_jobs()

```python
def get_jobs(df: Optional[DataFrame] = None, convert: bool = False) -> Union[DataFrame, List[BaseJob]]
```

```python
df = get_jobs()                          # All jobs as DataFrame
jobs = get_jobs(convert=True)            # All jobs as List[BaseJob]
jobs = get_jobs(df=filtered_df, convert=True)
```

DataFrame columns: `step`, `topic`, `item`, `job_id`, `options`, `tags`.

### get_step()

```python
def get_step(step: Union[TStep, str]) -> BaseStep
```

Use only for bulk/initial-setup operations. For individual jobs, prefer `get_job()`.

---

## BaseJob Methods

### Execution

- `job.run()` — full pipeline: pre-run checks → data processing → post-run checks
- `job.for_each_run()` — data processing only (skip invokers and checks)
- `job.overwrite()` — overwrite schema and run

### Schema Management

- `job.create()` — create the database object (table/view)
- `job.update_schema()` — update table schema to match current data structure
- `job.overwrite_schema()` — overwrite table schema completely
- `job.register()` — register an existing table/view

### Data Operations

- `job.truncate()` — remove all data
- `job.drop()` — drop the table/view

### Checks and Invokers

- `job.invoke_pre_run()` / `job.invoke_post_run()` — execute notebooks
- `job.check_pre_run()` / `job.check_post_run()` — execute data quality checks

### Properties

`job.step`, `job.topic`, `job.item`, `job.job_id`, `job.expand`, `job.mode`, `job.table`, `job.conf`

### Examples

```python
job = get_job(job="gold.sales_monthly_summary")
job.run()

job.truncate()
job.run()

job.for_each_run()   # Skip checks/invokers

# Gold UDFs
from fabricks.core.jobs import Gold
if isinstance(job, Gold):
    job.register_udfs()
job.run()
```

---

## BaseStep Methods

**Warning**: All step methods process every job in the step.

### Deployment

- `step.create()` — create/update step
- `step.update()` — update configs and create missing database objects
- `step.drop()` — drop step database and all objects

### Configuration

- `step.update_configurations()` — read YAML files and update `fabricks.{step}_jobs`
- `step.create_db_objects()` — create tables/views for all jobs
- `step.update_dependencies()` — resolve and store parent-child relationships

### Metadata

- `step.update_tables_list()`, `step.update_views_list()`, `step.update_steps_list()`

### Data Retrieval

- `step.get_jobs(topic=None)` — DataFrame of all jobs
- `step.get_dependencies(topic=None, include_manual=False)` — returns `(df, errors)`

### Properties

`step.name`, `step.expand`, `step.database`, `step.storage`, `step.runtime`, `step.workers`, `step.timeouts`, `step.options`, `step.conf`

---

## Runtime Configuration (conf.fabricks.yml)

```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope
  unity_catalog: true           # enable Unity Catalog mode
  catalog: my_catalog           # required when unity_catalog is true
  encryption_key: my_key        # optional column encryption key
  type_widening: true           # allow automatic type widening on schema evolution
  timeouts:
    step: 3600
    job: 3600
    pre_run: 3600
    post_run: 3600
  workers: 16                   # default: 16
  retention_days: 7
  timezone: Europe/Paris

path_options:
  storage: abfss://fabricks@$standard
  udfs: fabricks/udfs
  extenders: fabricks/extenders
  parsers: fabricks/parsers
  schedules: fabricks/schedules
  views: fabricks/views

spark_options:
  sql:
    spark.sql.parquet.compression.codec: zstd
    spark.sql.files.ignoreMissingFiles: true

bronze:
  - name: bronze
    path_options:
      runtime: bronze
      storage: abfss://bronze@$standard
    options:
      order: 101

silver:
  - name: silver
    path_options:
      runtime: silver
      storage: abfss://silver@$standard
    options:
      order: 201
      parent: bronze

gold:
  - name: gold
    path_options:
      runtime: gold/gold
      storage: abfss://gold@$standard/gold
    options:
      order: 301
      timeouts:
        job: 1800
```

### Configuration Precedence (highest → lowest)

1. Job-level options in YAML
2. Step-level defaults in runtime YAML
3. Global options in runtime YAML (`options`, `spark_options`, `path_options`)
4. `[tool.fabricks]` in `pyproject.toml`
5. Internal defaults

---

## Steps: Bronze, Silver, Gold

| Step | Purpose | Typical Modes | SQL Files | CDC |
|------|---------|---------------|-----------|-----|
| Bronze | Raw ingestion | `memory`, `append`, `register` | No | No |
| Silver | Clean/validate/CDC | `memory`, `append`, `latest`, `update`, `combine` | No | SCD0, SCD1, SCD2 |
| Gold | Business analytics | `memory`, `append`, `complete`, `update`, `invoke`, `register` | Required (except `invoke`/`register`) | SCD0, SCD1, SCD2 |

### Bronze

**Preferred pattern** — use `mode: register` with a pre-run invoker that writes the Delta file:

```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: register
      uri: abfss://fabricks@$datahub/raw/sales/daily_transactions
      keys: [transaction_id]
      source: pos_system
    invoker_options:
      pre_run:
        - notebook: bronze/invokers/ingest_sales
          arguments:
            source_url: https://api.example.com/sales
            target_path: abfss://fabricks@$datahub/raw/sales/daily_transactions
            date: "{{ today() }}"
```

The pre-run notebook fetches, transforms, and writes the Delta table; Fabricks then registers it.

**Alternative — direct file append:**
```yaml
options:
  mode: append
  uri: abfss://fabricks@$datahub/raw/sales
  parser: parquet
  keys: [transaction_id]
  source: pos_system
```

**Key Bronze options**: `mode`, `uri`, `parser`, `keys`, `source`, `invoker_options`

### Silver

Silver reads from parent jobs and applies CDC logic declaratively — **no SQL files**.

```yaml
- job:
    step: silver
    topic: sales_analytics
    item: daily_summary
    tags: [processed, sales]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.daily_transactions]
      filter_where: "status = 'active'"
      extender: sales_extender
      check_options:
        max_rows: 1000000
        min_rows: 10
```

**How Silver works**: reads parents → applies `filter_where` → applies `extender` → applies CDC → writes with `mode`.

**CDC columns**:
- `scd1`: adds `__is_current`, `__is_deleted`; creates `{table}__current` view
- `scd2`: adds `__valid_from`, `__valid_to` on top of SCD1 columns

**Key Silver options**: `mode`, `change_data_capture` (`nocdc`|`scd1`|`scd2`), `parents` (required), `filter_where`, `extender`, `check_options`

### Gold

Gold requires a SQL file named after the job `item` (e.g., `item: monthly_summary` → `monthly_summary.sql`). Heavy use of pre/post-run checks is strongly recommended for business-critical data.

```yaml
- job:
    step: gold
    topic: sales_reports
    item: monthly_summary
    tags: [report, sales]
    options:
      mode: complete
      change_data_capture: scd2
      parents: [silver.daily_summary]
    table_options:
      identity: true
      liquid_clustering: true
    check_options:
      min_rows: 1
      max_rows: 1000000
      pre_run: true
      post_run: true
```

**Example SQL** (`gold/gold/monthly_summary.sql`):
```sql
select
    date_trunc('month', transaction_date) as month,
    sum(amount) as total_sales,
    count(*) as transaction_count
from silver.daily_summary
where __is_current = true
group by month
```

**Check SQL format** — `{item}.pre_run.sql` / `{item}.post_run.sql` return:
- `__message`: error/warning text
- `__action`: `'fail'` (stops, rolls back) or `'warning'` (logs, continues)

```sql
-- gold/gold/monthly_summary.post_run.sql
select
    case
        when (select count(*) from gold.monthly_summary) = 0
            then 'No data in monthly_summary'
        when (select sum(total_sales) from gold.monthly_summary) < 0
            then 'Negative total_sales detected'
        else null
    end as __message,
    'fail' as __action
```

**SCD2 in Gold**: the base SELECT must include `__key`, `__timestamp` (date of change), and `__operation` (`'upsert'` or `'delete'`).

**Key Gold options**: `mode`, `change_data_capture`, `parents`, `table_options` (identity, liquid_clustering, properties), `invoker_options` (for `mode: invoke`), `check_options`

---

## Job Configuration Reference

```yaml
- job:
    step: gold
    topic: sales
    item: monthly_report
    tags: [report, sales]
    type: default                           # "default" | "manual" (manual skips auto-execution)
    options:
      mode: complete
      change_data_capture: scd2
      parents: [silver.daily_summary]
      filter_where: "status = 'active'"
      optimize: true
      compute_statistics: true
      timeout: 1800
    table_options:
      identity: true
      liquid_clustering: true
      properties:
        delta.minReaderVersion: 1
        delta.minWriterVersion: 7
    check_options:
      min_rows: 10
      max_rows: 1000000
      count_must_equal: fabricks.dummy
      pre_run: true
      post_run: true
    spark_options:
      sql:
        spark.databricks.delta.properties.defaults.targetFileSize: 1073741824
```

---

## Schedules

```yaml
# fabricks/schedules/schedule.yml
- schedule:
    name: nightly
    options:
      tag: nightly                           # Filter jobs by tag
      steps: [bronze, silver, gold]
      variables:
        run_date: "{{ today() }}"
      view: nightly                          # Optional: custom view to filter jobs
```

**Custom view** (`fabricks/views/nightly.sql`):
```sql
select * from fabricks.jobs j
where j.topic in ('sales', 'inventory')
  and array_contains(j.tags, 'nightly')
```

**Schedule execution flow**:
1. `generate(schedule="nightly")` — resolves job list, builds dependency graph
2. Steps execute in parallel, each running its jobs
3. `terminate(schedule_id=...)` — finalizes

Auto-generated view: `fabricks.{schedule_name}_schedule` — query to inspect resolved job list.

---

## Common Pipeline Example

**Bronze** — ingest with pre-run invoker:
```yaml
- job:
    step: bronze
    topic: sales
    item: transactions
    tags: [sales]
    options:
      mode: register
      uri: abfss://fabricks@$datahub/raw/sales/transactions
      keys: [transaction_id]
      source: pos_system
    invoker_options:
      pre_run:
        - notebook: bronze/invokers/ingest_sales
          arguments:
            target_path: abfss://fabricks@$datahub/raw/sales/transactions
            date: "{{ today() }}"
```

**Silver** — clean with CDC:
```yaml
- job:
    step: silver
    topic: sales
    item: transactions_cleaned
    tags: [sales]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.transactions]
      filter_where: "status in ('completed', 'pending')"
```

**Gold** — aggregate with checks:
```yaml
- job:
    step: gold
    topic: sales
    item: daily_summary
    tags: [sales]
    options:
      mode: complete
      parents: [silver.transactions_cleaned]
    check_options:
      min_rows: 1
      pre_run: true
      post_run: true
```

```sql
-- gold/gold/daily_summary.sql
select
    transaction_date,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    count(distinct customer_id) as unique_customers
from silver.transactions_cleaned
where __is_current = true
group by transaction_date
```

---

## Troubleshooting

1. **"could not resolve runtime"** — set `FABRICKS_RUNTIME` or add `[tool.fabricks]` to `pyproject.toml`; verify path exists.
2. **"conf mandatory"** — `conf.fabricks.yml` missing or YAML syntax error.
3. **Job dependencies not found** — verify parent format: `{step}.{topic}.{item}`; check parent defined in `_config.*.yml`.
4. **Schedule not finding jobs** — verify jobs have the required `tag`; confirm steps are listed in schedule `steps`; for custom views, ensure view returns `job_id`.
5. **CDC columns missing** — for Gold SCD2, ensure SQL returns `__key`, `__timestamp`, `__operation`.

**Debug**: set `loglevel = "debug"` or `FABRICKS_LOGLEVEL=DEBUG`. Query `fabricks.jobs`, `fabricks.{schedule_name}_schedule`, `fabricks.last_schedule`.

---

## Quick Reference

### Naming

- Job string: `{step}.{topic}_{item}` → e.g., `gold.sales_monthly_summary`
- Table name: `{step}.{topic}_{item}`

### Modes by Step

| Step | Modes |
|------|-------|
| Bronze | `memory`, `append`, `register` |
| Silver | `memory`, `append`, `latest`, `update`, `combine` |
| Gold | `memory`, `append`, `complete`, `update`, `invoke` |

### CDC

| Pattern | Columns added | Extra objects |
|---------|--------------|---------------|
| `nocdc` | — | — |
| `scd0`  | — | Static dimension; records inserted once, never updated |
| `scd1`  | `__is_current`, `__is_deleted` | `{table}__current` view (Silver) |
| `scd2`  | `__valid_from`, `__valid_to` + SCD1 columns | same |
