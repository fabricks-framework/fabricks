# Fabricks User Guide

Fabricks (Framework for Databricks) is a Python framework to define and run Lakehouse pipelines on Databricks using YAML for orchestration and SQL for business logic. This guide covers setup, core concepts, authoring jobs, execution, CDC, checks, and troubleshooting.

---

## Introduction

Fabricks is a pragmatic framework to build Databricks Lakehouse pipelines using YAML for orchestration and SQL for transformations. It reduces boilerplate (DDL/DML/merge), adds CDC and checks, and standardizes how teams define jobs, dependencies, and environments.

Who is it for?
- Data engineers who prefer SQL-first development on Databricks
- Teams seeking reproducible, versioned pipelines without heavyweight ETL tools

What you get:
- Declarative jobs, schedules, and step configuration
- CDC (SCD1, SCD2), quality checks, logging, dependency management
- Extensibility via Python extenders and UDFs

## Overview

- **What it is**: A standardized way to manage ETL pipelines (bronze/silver/gold) with reproducible jobs, schedules, and validation.
- **How you write logic**: Mostly SQL; Fabricks generates DDL/DML/merge and manages table/view lifecycles.
- **Where it runs**: Optimized for Azure Databricks; uses Azure Blob, Table, and Queue services.

---

## Table of Contents

- Introduction
- Overview
- Installation
- Quickstart
- Project Configuration
- Core Concepts
- Step Modes Summary
- Authoring Jobs (Bronze, Silver, Gold)
- Checks and Data Quality
- SCD2 in Gold (pattern)
- Execution Model
- Running on Databricks
- Practical examples from tests
- Job options reference
- Sample runtime
- Local Development and Testing
- Extenders, UDFs, and Views
- Logging and Monitoring
- Troubleshooting
- Compatibility
- License

## Quick Links

- Step overviews: [Bronze](./steps/bronze.md), [Silver](./steps/silver.md), [Gold](./steps/gold.md)
- Detailed step references: [Bronze](./steps/bronze.md), [Silver](./steps/silver.md), [Gold](./steps/gold.md)
- Sample runtime: [examples/runtime](./examples/runtime/)
- Integration examples: `framework/tests/integration/runtime/`

## Installation

- From PyPI in Databricks: install the `fabricks` wheel/library on your cluster.
- Python: requires `>=3.9,<4`. Align dependencies to Databricks LTS runtimes.
- Local dev (suggested):
  - With uv: `uv sync` then `uv run pytest`
  - With pip: `pip install -e .[dev,test]`

Key dependencies: `databricks-sdk`, `jinja2`, `pyyaml`, `sqlglot`, `azure-identity`, `azure-storage-*`.

---

## Quickstart

1) Point Fabricks to your runtime in `pyproject.toml` (or use the sample):
   - runtime: path to your jobs/SQL/configs
   - config: your `conf.fabricks.yml`
2) Create a schedule (e.g., `example`) with ordered steps.
3) Author a simple job (e.g., gold `hello_world.sql`).
4) Run using the Databricks bundle job or notebooks, passing `schedule`.
5) Inspect outputs (tables/views) and logs (Fabricks log tables).

See Sample runtime below to copy a working structure.

## Project Configuration

Define Fabricks settings in `pyproject.toml`:

```toml
[tool.fabricks]
runtime = "tests/integration/runtime"          # Path to your runtime (jobs, SQL, configs)
notebooks = "fabricks/api/notebooks"           # Notebook helpers shipped with Fabricks
job_config_from_yaml = true                     # Enable YAML job config
loglevel = "info"
debugmode = false
config = "tests/integration/runtime/fabricks/conf.uc.fabricks.yml"  # Main runtime config
```

Runtime configuration (YAML) drives steps, timeouts, Spark options, and storage locations. Example:

```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope
  timeouts:
    step: 3600
    job: 3600
    pre_run: 3600
    post_run: 3600
  workers: 4
path_options:
  storage: /mnt/data
spark_options:
  sql:
    spark.sql.parquet.compression.codec: zstd

bronze:
  - name: bronze
    path_options:
      runtime: src/steps/bronze
      storage: abfss://bronze@youraccount.blob.core.windows.net

silver:
  - name: silver
    path_options:
      runtime: src/steps/silver
      storage: abfss://silver@youraccount.blob.core.windows.net

gold:
  - name: transf
    path_options:
      runtime: src/steps/transf
      storage: abfss://transf@youraccount.blob.core.windows.net
  - name: gold
    path_options:
      runtime: src/steps/gold
      storage: abfss://gold@youraccount.blob.core.windows.net
```

---

## Core Concepts

- **Runtime**: Directory where your Lakehouse lives (configs, SQL, jobs, UDFs, extenders).
- **Step**: Layer in the pipeline, commonly `bronze` → `silver` → `gold`. You can add more (e.g., `transf`, `semantic`).
- **Job**: A unit of work (generally one table or view). Jobs can depend on others via `parents`.
- **Schedule**: Named grouping of jobs to execute (e.g., `test`, `daily`).
- **CDC**: Change Data Capture strategy in `silver` or `gold` (supports `nocdc`, `scd1`, `scd2`).

---

## Step Modes Summary

- Bronze
  - memory: Temporary view only; no table written.
  - append: Append data; no merges.
  - register: Register existing table at uri.
- Silver
  - memory: View only.
  - append: Append-only.
  - latest: Keep latest per key.
  - update: Merge/upsert; with CDC supports scd1/scd2.
  - combine: Combine parent outputs.
- Gold (and transf)
  - memory, append, complete, update, invoke (notebook-run).
- Semantic
  - complete: Apply properties/metadata/copy.

## Authoring Jobs

Jobs are typically defined in YAML and/or SQL files in the runtime. See `framework/tests/integration/runtime` for examples.

### Step modes reference (at a glance)

- Bronze
  - memory: Build a temporary view only; no Delta table is created or mutated.
  - append: Append new data into the target Delta table (no upserts/merges).
  - register: Register an existing Delta table at `uri`; metadata-only (no data movement).

- Silver
  - memory: Produce a view only; nothing is persisted.
  - append: Append rows to the target table (good for log-like data).
  - latest: Keep the latest row per key (dedup within batch to a single “current” row).
  - update: Merge/upsert into the target table. With CDC:
    - scd1: Maintains current snapshot with flags; overwrites current rows for a key.
    - scd2: Maintains history with validity windows.
  - combine: Combine/union outputs from multiple parent jobs into one result.

- Gold (also applies to transf steps)
  - memory: View only; use for ad‑hoc/ephemeral outputs.
  - append: Append rows to the target table.
  - complete: Full refresh; replace the table with the current result.
  - update: Merge/upsert into the target table. With CDC:
    - scd1: Current snapshot with delete/current flags.
    - scd2: Historical table with validity windows.
  - invoke: Run a notebook instead of SQL; behavior defined by `invoker_options`.

- Semantic
  - complete: Apply table properties/metadata and (optionally) copy from `options.table`.

### Bronze
[Step reference](./steps/bronze.md)

Raw ingestion from files, streams, or pre-run notebooks.

```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: append               # append/register
      uri: abfss://fabricks@$datahub/raw/sales
      parser: parquet            # autoloader
      keys: [transaction_id]
      source: pos_system
```

Pre-run notebook example:

```yaml
- job:
    step: bronze
    topic: apidata
    item: http_api
    options:
      mode: register             # directly register a Delta table
      uri: abfss://fabricks@$datahub/raw/http_api
      keys: [transaction_id]
    invoker_options:
      pre_run:
        - notebook: bronze/invokers/http_call
          arguments:
            url: https://shouldideploy.today
```

Where to go next: See the Bronze step reference for all modes, options, and parser usage.

### Silver
[Step reference](./steps/silver.md)

Standardizes and enriches data; can apply CDC (SCD1/SCD2).

```yaml
- job:
    step: silver
    topic: sales_analytics
    item: daily_summary
    options:
      mode: update
      change_data_capture: scd1  # scd1 | scd2 | nocdc
      parents: [bronze.daily_transactions]
      extender: sales_extender
      check_options:
        max_rows: 1000000
```

SCD1 adds `__is_current`, `__is_deleted`. SCD2 adds `__valid_from`, `__valid_to`. A `TABLE__current` view is created for convenience.

### Gold
[Step reference](./steps/gold.md)

Downstream consumption models, often pure SQL; semantic applies table properties/metadata.

```yaml
- job:
    step: gold
    topic: sales_reports
    item: monthly_summary
    options:
      mode: complete
      change_data_capture: scd2
```

---

## Job options reference

Below is a brief description of common options per step and their effects.


### Cross-cutting options

- table_options: Controls Delta table behavior
  - identity: Adds `__identity` column for Delta identity columns.
  - liquid_clustering: Enable Liquid Clustering where available.
  - partition_by/zorder_by/cluster_by/bloomfilter_by: Physical layout optimizations.
  - constraints: Map of constraints (e.g., `primary key(__key)`).
  - properties: Delta table properties (e.g., `delta.enableChangeDataFeed: true`).
  - comment: Table comment/description.
  - calculated_columns: Add derived columns at write time.
  - retention_days: Table retention period for VACUUM.
- check_options: Data quality checks
  - pre_run/post_run: Execute SQL checks that return `__action`, `__message`.
  - min_rows/max_rows: Enforce row count bounds.
  - count_must_equal: Ensure row count equals another table.
- spark_options: Per-job Spark config
  - sql/conf: Key-value pairs applied to the Spark session for this job.
- invoker_options: Configure notebooks to run
  - pre_run/post_run: Arrays of notebooks to execute around the job.
  - run: Single notebook to execute when using `mode: invoke`.
- extender_options: Apply custom Python extenders with optional arguments.

Notes:
- Step-level defaults come from the runtime `conf.fabricks.yml`. Job-level options override step defaults.
- `memory` mode never writes a physical Delta table; features like rollback or table properties don’t apply.
- Checks that fail on physical tables trigger automatic rollback to the previous successful version.



## Checks and Data Quality

Use `check_options` to enforce expectations:

```yaml
check_options:
  min_rows: 10
  max_rows: 10000
  count_must_equal: fabricks.dummy
  pre_run: true
  post_run: true
```

See also:
- Examples: `framework/examples/runtime/gold/gold/_config.example.yml`, `framework/examples/runtime/gold/gold/hello_world.sql`
- Rich scenarios: `framework/tests/integration/runtime/gold/gold/`

If `pre_run`/`post_run` is enabled, provide `table_name.pre_run.sql` / `table_name.post_run.sql` that returns:
- `__message`: error message
- `__action`: `'fail'` or `'warning'`

On failure for physical tables, Fabricks restores data to the previous version; for `memory` (views), it logs only.

---

## Execution Model

Fabricks ships utility notebooks to orchestrate runs:

- `initialize.py`: reads `schedule`, generates a unique `schedule_id`, and materializes jobs/dependencies for that schedule.
- `process.py`: runs a specific `step` for the current `schedule_id` (invoked per step task).
- `terminate.py`: finalizes the run for the `schedule_id`.
- `cluster.py`: simple utility to exit successfully (often a cluster warm-up task).

The underlying DAG generation (`fabricks.core.dags.generator.DagGenerator`) builds job lists and dependencies from Spark SQL sources (e.g., `fabricks.jobs`, `fabricks.<schedule>_schedule`), writes logs (`fabricks.logs_pivot`), and seeds Azure Queues per step for coordinated processing.

---

## Running on Databricks

Use the provided Databricks bundle/job definition (`framework/databricks.yml`):

- `fabricks_test_job`: prepares a cluster and (optionally) runs tests.
- `fabricks_run_job`: orchestrates a full run with tasks: `cluster` → `initialize` → parallel step `process` tasks → `terminate`.
- Parameters: `schedule` (default `test`).

Typical flow:
1) Build wheel: `uv build` (bundle does this via `artifacts.fabricks_tool`).
2) Deploy bundle and run the job with your target `schedule`.

---

## Practical examples from tests

Below are distilled examples taken from `framework/tests/integration` to show common patterns.

### Schedules

Define the schedule and step order in your runtime:

```yaml
- schedule:
    name: test
    options:
      tag: test
      steps: [bronze, silver, transf, gold, semantic]
      variables:
        var1: 1
        var2: 2
```

Run with the provided notebooks or the bundle job, passing `schedule: test`.




### Dependencies

SQL-based dependency discovery can reference existing views/tables. Example:

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

Update a job’s dependencies programmatically:

```python
from fabricks.core import get_job
j = get_job(step="gold", topic="fact", item="dependency_sql")
j.update_dependencies()
```

Notebook-based dependencies can publish a UUID view and exit with that ID (see `dependency_notebook.ipynb`).

### Invoke (notebooks as part of jobs)

Pre-, post-, and direct-run invocations with timeouts:

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
    item: failed_pre_run
    options: { mode: memory }
    invoker_options:
      pre_run:
        - notebook: gold/gold/invoke/pre_run/exe
          arguments: { arg1: 1 }
- job:
    step: gold
    topic: invoke
    item: timedout_pre_run
    options: { mode: memory }
    invoker_options:
      pre_run:
        - timeout: 10
          notebook: gold/gold/invoke/timedout
          arguments: { arg1: 1 }
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


### Checks

Define data quality checks per job:

```yaml
- job:
    step: gold
    topic: check
    item: fail
    options: { mode: complete }
    check_options: { pre_run: true }
- job:
    step: gold
    topic: check
    item: warning
    options: { mode: complete }
    check_options: { post_run: true }
- job:
    step: gold
    topic: check
    item: max_rows
    options: { mode: complete }
    check_options: { max_rows: 2 }
- job:
    step: gold
    topic: check
    item: min_rows
    options: { mode: complete }
    check_options: { min_rows: 2 }
- job:
    step: gold
    topic: check
    item: count_must_equal
    options: { mode: complete }
    check_options: { count_must_equal: fabricks.dummy }
- job:
    step: gold
    topic: check
    item: skip
    options: { mode: complete }
    check_options: { skip: true }
```

Check SQL contracts:

```sql
-- fail.pre_run.sql
select "fail" as __action, "Please don't fail on me :(" as __message

-- warning.post_run.sql
select "I want you to warn me !" as __message, "warning" as __action
```

On a failed check, physical tables are rolled back to the previous version.

### UDFs

Register UDFs in your runtime and use them in SQL:

```python
from fabricks.core.udfs import udf
from pyspark.sql import SparkSession

@udf(name="parse_phone_number")
def parse_phone_number(spark: SparkSession):
    ...  # register spark.udf with a StructType
```

Using identity/key and custom addition UDFs:

```sql
select
  udf_identity(s.id, id) as __identity,
  udf_key(array(s.id)) as __key,
  s.id as id,
  s.name as monarch,
  udf_additition(1, 2) as addition
from silver.monarch_scd1__current s
```

### Semantic step

Per-step defaults (from runtime config) and per-job overrides:

```yaml
- job:
    step: semantic
    topic: fact
    item: step_option
    options: { mode: complete }
- job:
    step: semantic
    topic: fact
    item: job_option
    options: { mode: complete }
    table_options:
      properties:
        delta.minReaderVersion: 2
        delta.minWriterVersion: 5
        delta.columnMapping.mode: none
- job:
    step: semantic
    topic: fact
    item: powerbi
    options: { mode: complete }
    table_options: { powerbi: true }
```

Compression verification example (files should contain `zstd`): use the `zstd` job to write a small table, then list Delta files and check for `zstd` in file names.

---

## Sample runtime

Use this minimal runtime as a starting point. Point `tool.fabricks.runtime` to this folder and set `config` to the included `conf.fabricks.yml`.

Tree:

```
examples/runtime/
  fabricks/
    conf.fabricks.yml
    schedules/
      schedule.yml
  bronze/
    _config.example.yml
  silver/
    _config.example.yml
  gold/
    gold/
      _config.example.yml
      hello_world.sql
  semantic/
    _config.example.yml
```

Key files:

- `fabricks/conf.fabricks.yml`

```yaml
- conf:
    name: getting-started
    options:
      secret_scope: my_scope
      timeouts:
        step: 1800
        job: 1800
      workers: 2
    path_options:
      storage: /mnt/demo
      schedules: fabricks/schedules
    gold:
      - name: gold
        path_options:
          runtime: gold/gold
        options:
          order: 301
```

- `fabricks/schedules/schedule.yml`

```yaml
- schedule:
    name: example
    options:
      steps: [gold]
```

- `gold/gold/_config.example.yml`

```yaml
- job:
    step: gold
    topic: demo
    item: hello_world
    tags: [example]
    options:
      mode: complete
```

- `gold/gold/hello_world.sql`

```sql
select 1 as __key, 'hello' as greeting
```



Run the bundle job with `schedule: example` to materialize `gold.demo_hello_world`.

## Local Development and Testing

- Type checking: `pyright` (configured for Python 3.10).
- Linting/formatting: `ruff`, `autoflake`, `isort`, `sqlfmt`, `yamlfix`.
- Tests: `pytest`. Integration tests demonstrate job SQL generation under `framework/tests/integration`.

Suggested workflow:
- Create/modify runtime SQL and YAML in your runtime folder.
- Add tests mirroring expected SQL in `tests/integration/expected`.
- Run `pytest` locally or in Databricks using the shipped notebooks/jobs.

---

## Extenders, UDFs, and Views

- UDFs: Place in your runtime under `fabricks/udfs`; register in notebooks or initialization code as needed.
- Extenders: Custom Python to enrich processing (e.g., `extenders/add_country.py` in integration runtime examples).
- Views: Ship reusable SQL views under `fabricks/views` and reference them in jobs.

Quick examples

Extender (adds a country column):
```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from fabricks.core.extenders import extender

@extender(name="add_country")
def add_country(df: DataFrame, **kwargs) -> DataFrame:
    return df.withColumn("country", lit(kwargs.get("country", "Unknown")))
```

UDF (simple addition):
```python
from pyspark.sql import SparkSession
from fabricks.core.udfs import udf

@udf(name="additition")
def additition(spark: SparkSession):
    def _add(a: int, b: int) -> int:
        return a + b
    spark.udf.register("udf_additition", _add)
```

Parser (custom file reader/transformer):
What a parser should do
- Read raw data from `data_path` (batch or stream based on `stream` flag)
- Optionally apply/validate a schema from `schema_path`
- Perform light, source-specific cleanup (drops/renames/type fixes)
- Return a Spark DataFrame (do not write output or mutate state)
- Be idempotent and fast; avoid heavy business logic (keep that in Silver/Gold)

Inputs
- data_path: points to the source location (directory or file)
- schema_path: optional schema location (e.g., JSON/DDL); may be ignored
- spark: active `SparkSession`
- stream: when true, prefer `readStream` semantics if supported

Referencing in a job
```yaml
- job:
    step: bronze
    topic: demo
    item: source
    options:
      mode: append
      uri: /mnt/demo/raw/demo
      parser: monarch
    parser_options:
      file_format: parquet
      read_options:
        mergeSchema: true
```

```python
from pyspark.sql import DataFrame, SparkSession
from fabricks.core.parsers import parser
from fabricks.utils.path import Path

@parser(name="monarch")
class MonarchParser:
    def parse(self, data_path: Path, schema_path: Path, spark: SparkSession, stream: bool) -> DataFrame:
        # read using built-ins or your logic; example with parquet
        df = spark.read.parquet(data_path.string)
        # apply light cleansing, drops, renames, schema application, etc.
        cols_to_drop = [c for c in df.columns if c.startswith("BEL_")]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df
```

---

## Logging and Monitoring

- Logs are written to Fabricks tables (e.g., a pivoted view of execution in `fabricks.logs_pivot`).
- Status transitions (e.g., `scheduled`) are recorded with timestamps and median durations.
- A table-backed log handler and Azure Table Storage are utilized for durable logging; Azure Queues coordinate per-step work.

---

## Troubleshooting

- Missing parameters: Notebooks assert that `schedule` and/or `step` are provided; ensure bundle/job passes them.
- Permissions: Ensure the workspace has access to storage accounts (Blob/Table/Queue) and secret scopes.
- Dependency alignment: Keep library versions compatible with the Databricks LTS runtime.
- Circular dependencies: Fabricks filters `dependencies_circular`; review parents to avoid cycles.

---

## Compatibility

- Python: `>=3.9,<4` (tooling targets 3.10).
- Databricks: example configs target Spark `15.4.x-scala2.12`.
- Azure: requires access to Blob, Table, and Queue services.

---

### Examples

Starter runtime with bronze/silver/transf/gold/semantic examples is available under `framework/examples/runtime/`.

- Configuration: `framework/examples/runtime/fabricks/conf.fabricks.yml`
- Schedule: `framework/examples/runtime/fabricks/schedules/schedule.yml`
- Bronze jobs: `framework/examples/runtime/bronze/_config.example.yml`
- Silver jobs: `framework/examples/runtime/silver/_config.example.yml`
- Transf jobs: `framework/examples/runtime/gold/transf/_config.example.yml`
- Gold jobs: `framework/examples/runtime/gold/gold/_config.example.yml`, SQL under `hello_world.sql`
- Semantic jobs: `framework/examples/runtime/semantic/_config.example.yml`


## License

MIT License. See repository license for details.
