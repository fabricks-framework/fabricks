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

Create a `fabricksconfig.json` file in your project root:

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

**Configuration fields**:
- `runtime`: Path to runtime directory (relative to config file location)
- `notebooks`: Optional path to helper notebooks
- `job_config_from_yaml`: Enable YAML job configs (boolean)
- `loglevel`: Log level - `"debug"` | `"info"` | `"warning"` | `"error"` | `"critical"`
- `debugmode`: Enable debug mode (boolean)
- `devmode`: Enable dev mode (boolean)
- `funmode`: Enable fun mode (boolean)
- `config`: Path to main runtime YAML file (relative to config file location)

**Option B: pyproject.toml (alternative)**

If `fabricksconfig.json` is not found, Fabricks will check for `pyproject.toml`:

```toml
[tool.fabricks]
runtime = "path/to/your/runtime"              # Path to runtime directory
notebooks = "fabricks/api/notebooks"          # Optional: helper notebooks
job_config_from_yaml = true                    # Enable YAML job configs
loglevel = "info"                              # DEBUG|INFO|WARNING|ERROR|CRITICAL
debugmode = false
config = "path/to/runtime/fabricks/conf.fabricks.yml"  # Main runtime YAML
```

**Option C: Environment Variables**

If no config files are found, use environment variables:

```python
FABRICKS_RUNTIME=/Workspace/Repos/your/repo/runtime
FABRICKS_CONFIG=/Workspace/Repos/your/repo/runtime/fabricks/conf.fabricks.yml
FABRICKS_NOTEBOOKS=/Workspace/Repos/your/repo/notebooks
FABRICKS_LOGLEVEL=INFO
FABRICKS_IS_JOB_CONFIG_FROM_YAML=true
FABRICKS_IS_DEBUGMODE=false
FABRICKS_IS_DEVMODE=false
FABRICKS_IS_FUNMODE=false
```

**Note**: Fabricks searches for config files by walking up the directory tree from the current working directory. The first `fabricksconfig.json` or `pyproject.toml` found will be used.

### 3. Runtime Structure

Typical runtime directory structure:
```
runtime/
  fabricks/
    conf.fabricks.yml          # Main runtime configuration
    schedules/
      schedule.yml             # Schedule definitions
    views/                      # Optional: SQL views for job filtering
    udfs/                       # Optional: User-defined functions
    extenders/                  # Optional: Python extenders
    parsers/                    # Optional: Custom parsers
  bronze/
    _config.example.yml         # Bronze job configurations
  silver/
    _config.example.yml         # Silver job configurations (no SQL files - reads from parents)
  gold/
    gold/
      _config.example.yml       # Gold job configurations
      *.sql                     # SQL transformation files (required for Gold)
```

### 4. Initialization (armageddon)

Run `armageddon` once to bootstrap metadata, databases, and views:

```python
from fabricks.core.scripts.armageddon import armageddon

# Initialize all steps from runtime config
armageddon()

# Or initialize specific steps
armageddon(steps=["bronze", "silver", "gold"])
armageddon(steps="gold")
```

**Note**: `armageddon` is a wrapper around `Deploy.armageddon()`. See the [Deploy Class](#deploy-class) section for more details.

---

## Deploy Class

The `Deploy` class provides methods to deploy and manage Fabricks infrastructure components, including tables, views, UDFs, masks, notebooks, schedules, and steps. It's essential for setting up and maintaining your Fabricks environment.

### Import

```python
from fabricks.deploy import Deploy
```

### Methods

#### `Deploy.tables(drop: bool = False)`

Creates or replaces Fabricks system tables in the `fabricks` database:
- `fabricks.logs` - Execution logs for schedules, steps, and jobs
- `fabricks.steps` - Step metadata (step name, expand type, execution order)
- `fabricks.dummy` - Dummy table used for data quality checks (e.g., `count_must_equal`)

**Parameters**:
- `drop`: If `True`, drops existing tables before creating new ones

**Usage**:
```python
Deploy.tables()              # Create/replace tables
Deploy.tables(drop=True)     # Drop and recreate tables
```

#### `Deploy.views()`

Creates or replaces system views in the `fabricks` database. These views provide convenient access to job metadata, dependencies, and execution history:

- `fabricks.jobs` - Unified view of all jobs across all steps
- `fabricks.tables` - List of all tables created by jobs
- `fabricks.views` - List of all views created by jobs
- `fabricks.dependencies` - Job dependency relationships
- `fabricks.dependencies_flat` - Flattened dependency view
- `fabricks.dependencies_circular` - Detects circular dependencies
- `fabricks.last_schedule` - Most recent schedule execution
- `fabricks.last_status` - Latest execution status per job
- `fabricks.schedules` - All schedule definitions
- `fabricks.logs_pivot` - Pivoted log view for easier analysis

**Usage**:
```python
Deploy.views()
```

#### `Deploy.udfs(override: bool = True)`

Deploys User-Defined Functions (UDFs) from the runtime `udfs` directory:
- Registers all SQL UDFs found in `fabricks/udfs/`
- Creates `fabricks.udf_job_id()` function for job ID hashing

**Parameters**:
- `override`: If `True`, replaces existing UDFs with the same name

**Usage**:
```python
Deploy.udfs()                # Deploy UDFs, override existing
Deploy.udfs(override=False)   # Skip if UDF already exists
```

#### `Deploy.masks(override: bool = True)`

Deploys data masking functions from the runtime `masks` directory. Masks are used for data privacy and anonymization.

**Parameters**:
- `override`: If `True`, replaces existing masks with the same name

**Usage**:
```python
Deploy.masks()                # Deploy masks, override existing
Deploy.masks(override=False) # Skip if mask already exists
```

#### `Deploy.notebooks()`

Deploys helper notebooks to the Databricks workspace. These notebooks are copied from the Fabricks package to your configured `notebooks` path:

- `cluster.py` - Cluster management
- `initialize.py` - Initialization helper
- `process.py` - Step processing
- `schedule.py` - Schedule execution
- `run.py` - Job execution
- `terminate.py` - Schedule termination

**Usage**:
```python
Deploy.notebooks()
```

**Note**: This overwrites existing notebooks in the target directory.

#### `Deploy.schedules()`

Deploys schedule views and custom views:
- Creates schedule views (`fabricks.{schedule_name}_schedule`) for each schedule
- Creates custom views from SQL files in `fabricks/views/`

**Usage**:
```python
Deploy.schedules()
```

#### `Deploy.armageddon(steps, nowait: bool = False)`

**Full deployment and initialization** - This is the most comprehensive deployment method. It performs a complete reset and setup of your Fabricks environment.

**What it does**:
1. **Drops everything**:
   - Drops `fabricks` database
   - Drops all step databases (bronze, silver, gold, etc.)
   - Removes temporary storage folders (`tmp`, `checkpoints`, `schemas`, `schedules`)

2. **Recreates infrastructure**:
   - Creates `fabricks` database
   - Deploys system tables (`Deploy.tables(drop=True)`)
   - Deploys UDFs (`Deploy.udfs()`)
   - Deploys masks (`Deploy.masks()`)
   - Deploys notebooks (`Deploy.notebooks()`)

3. **Deploys steps**:
   - For each step, calls `BaseStep.create()` which:
     - Creates the step database
     - Updates job configurations from YAML files
     - Creates database objects (tables/views) for all jobs
     - Updates dependencies
     - Updates tables and views lists
     - Updates step metadata

4. **Finalizes**:
   - Deploys views (`Deploy.views()`)
   - Deploys schedules (`Deploy.schedules()`)

**Parameters**:
- `steps`: Optional. Can be:
  - `None` - Deploys all steps from runtime config
  - Single step: `"gold"` or `["gold"]`
  - Multiple steps: `["bronze", "silver", "gold"]`
- `nowait`: If `True`, skips the atomic bomb warning delay

**Usage**:
```python
# Deploy all steps
Deploy.armageddon(steps=None)

# Deploy specific steps
Deploy.armageddon(steps=["bronze", "silver", "gold"])
Deploy.armageddon(steps="gold")

# Skip warning delay
Deploy.armageddon(steps=None, nowait=True)
```

**Warning**: `armageddon` is destructive - it drops and recreates everything. Use with caution in production!

### Deployment Workflow

Typical deployment workflow:

1. **Initial Setup** (one-time):
   ```python
   from fabricks.deploy import Deploy
   
   # Full initialization
   Deploy.armageddon(steps=None)
   ```

2. **Update Individual Components**:
   ```python
   # After adding new UDFs
   Deploy.udfs()
   
   # After adding new views
   Deploy.views()
   
   # After updating schedules
   Deploy.schedules()
   ```

3. **Update Step** (after modifying job configs):

   **For a single job (preferred - much faster)**:
   ```python
   from fabricks.api import get_job
   
   job = get_job(step="gold", topic="sales", item="new_report")
   job.create()  # Only processes this one job
   ```

   **For multiple jobs (bulk - can be slow)**:
   ```python
   from fabricks.api import get_step
   
   step = get_step("gold")
   step.update()  # Processes ALL jobs in step - can take significant time
   ```

### Step Deployment Details

When `Deploy.armageddon()` or `BaseStep.create()` is called, it:

1. **Creates step database** (e.g., `gold`, `silver`, `bronze`)
2. **Updates configurations**: Reads YAML files and updates `fabricks.{step}_jobs` table
3. **Creates database objects**: For each job in the step:
   - Creates tables/views based on job `mode` and `type`
   - Applies table options (identity, clustering, properties)
   - Sets up CDC structures if configured
4. **Updates dependencies**: Resolves and stores parent-child relationships
5. **Updates metadata**: Updates `fabricks.{step}_tables`, `fabricks.{step}_views`, `fabricks.steps`

### Common Deployment Scenarios

**Scenario 1: New Project Setup**
```python
from fabricks.deploy import Deploy

# Complete initialization
Deploy.armageddon(steps=None)
```

**Scenario 2: Adding New Jobs to Existing Step**
```python
from fabricks.api import get_step

# Update step (reads new YAML configs and creates missing objects)
step = get_step("gold")
step.update()
```

**Scenario 3: Updating System Components**
```python
from fabricks.deploy import Deploy

# After modifying UDFs, views, or schedules
Deploy.udfs()
Deploy.views()
Deploy.schedules()
```

**Scenario 4: Recreating Everything (Clean Slate)**
```python
from fabricks.deploy import Deploy

# Nuclear option - drops and recreates everything
Deploy.armageddon(steps=None, nowait=True)
```

### Related Methods

The `BaseStep` class also provides deployment-related methods. **Note**: These operate on ALL jobs in the step and can be time-consuming.

- `step.create()` - Creates/updates step (calls `step.update()`) - processes all jobs
- `step.update()` - Updates configurations and creates missing database objects - processes all jobs
- `step.drop()` - Drops step database and all related objects - processes all jobs
- `step.update_configurations()` - Updates job configurations from YAML - processes all jobs
- `step.create_db_objects()` - Creates tables/views for all jobs - processes all jobs
- `step.update_dependencies()` - Updates job dependency relationships - processes all jobs

**Best Practice**: For individual job operations, use `get_job()` instead:
- `job.create()` instead of `step.create_db_objects()` for a single job
- `job.run()` instead of processing entire step
- `job.update_schema()` instead of updating all jobs in step

---

## Main API Functions

Fabricks provides core API functions to work with jobs and steps. These are the primary entry points for programmatic interaction with Fabricks.

**⚠️ Performance Best Practice**: **Prefer job-level interaction** (`get_job()`, `get_jobs()`) over step-level interaction (`get_step()`). Step-level operations process all jobs in a step and can take significant time (minutes to hours). Use step-level operations only for bulk operations or initial setup.

### Import

```python
from fabricks.api import get_job, get_jobs, get_step
```

---

## get_job()

Retrieves a single job instance. Returns a `BaseJob` object (or `Bronze`, `Silver`, or `Gold` subclass).

### Function Signature

```python
def get_job(
    job: Optional[str] = None,
    step: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    job_id: Optional[str] = None,
    row: Optional[Row] = None,
) -> BaseJob
```

### Parameters

Multiple ways to identify a job:

1. **By job string** (recommended):
   ```python
   job = get_job(job="gold.sales_monthly_summary")
   ```
   Format: `{step}.{topic}_{item}` (e.g., `gold.sales_monthly_summary`)

2. **By step, topic, item**:
   ```python
   job = get_job(step="gold", topic="sales", item="monthly_summary")
   ```

3. **By step and job_id**:
   ```python
   job = get_job(step="gold", job_id="abc123...")
   ```

4. **From a DataFrame row**:
   ```python
   df = spark.sql("select * from fabricks.jobs where step = 'gold'")
   row = df.first()
   job = get_job(row=row)
   ```

### Returns

- `BaseJob` instance (or `Bronze`, `Silver`, `Gold` subclass)
- The returned object has access to all job methods (see [BaseJob Methods](#basejob-methods) below)

### Examples

```python
from fabricks.api import get_job

# Get a gold job
job = get_job(job="gold.sales_monthly_summary")

# Get a bronze job
job = get_job(step="bronze", topic="sales_data", item="daily_transactions")

# Get a silver job
job = get_job(step="silver", topic="sales", item="cleaned_transactions")
```

---

## get_jobs()

Retrieves multiple jobs, either as a DataFrame or as a list of `BaseJob` objects.

### Function Signature

```python
def get_jobs(
    df: Optional[DataFrame] = None,
    convert: Optional[bool] = False
) -> Union[List[BaseJob], DataFrame]
```

### Parameters

- `df`: Optional DataFrame to filter jobs. If `None`, returns all jobs from all steps.
- `convert`: If `True`, returns a list of `BaseJob` objects. If `False` (default), returns a DataFrame.

### Returns

- If `convert=False`: Returns a `DataFrame` with job information
- If `convert=True`: Returns a `List[BaseJob]` of job objects

### DataFrame Format

When `convert=False`, the DataFrame contains:
- `step` - Step name (bronze, silver, gold, etc.)
- `topic` - Topic identifier
- `item` - Item identifier
- `job_id` - Hashed job ID
- `options` - Job options (nested struct)
- `tags` - Job tags (array)

### Examples

```python
from fabricks.api import get_jobs

# Get all jobs as DataFrame
df = get_jobs()
df.show()

# Filter jobs from a DataFrame
filtered_df = spark.sql("select * from fabricks.jobs where step = 'gold'")
jobs = get_jobs(df=filtered_df, convert=True)  # Returns List[BaseJob]

# Get all jobs as objects
all_jobs = get_jobs(convert=True)  # Returns List[BaseJob]

# Work with specific jobs
for job in all_jobs:
    if job.topic == "sales":
        print(f"Job: {job}")
```

---

## get_step()

Retrieves a step instance. Returns a `BaseStep` object.

**⚠️ Performance Warning**: Step-level operations can be **very time-consuming** as they process all jobs in the step. **Prefer job-level interaction** (`get_job()`) for individual job operations. Use step-level operations only when you need to:
- Update configurations for all jobs in a step
- Create database objects for multiple jobs at once
- Update dependencies across an entire step
- Perform bulk operations during initial setup

### Function Signature

```python
def get_step(step: Union[TStep, str]) -> BaseStep
```

### Parameters

- `step`: Step name (e.g., `"bronze"`, `"silver"`, `"gold"`, or any custom step name from runtime config)

### Returns

- `BaseStep` instance with access to all step methods (see [BaseStep Methods](#basestep-methods) below)

### When to Use Step-Level Operations

- **Initial setup/deployment**: Use `Deploy.armageddon()` or `step.update()` during initial project setup
- **Bulk configuration updates**: When you've modified multiple job YAML files and need to update all at once
- **Dependency updates**: When parent relationships have changed across many jobs
- **Metadata updates**: When you need to refresh step-level metadata lists

### When to Use Job-Level Operations (Preferred)

- **Individual job operations**: Running, creating, or updating a specific job
- **Testing/debugging**: Working with a single job during development
- **Incremental updates**: Adding or modifying one job at a time
- **Daily operations**: Most common operational tasks

### Examples

```python
from fabricks.api import get_step

# Get a step (use sparingly - prefer get_job() for individual jobs)
step = get_step("gold")

# Only use step.update() for bulk operations or initial setup
# For individual jobs, prefer:
from fabricks.api import get_job
job = get_job(job="gold.sales_monthly_summary")
job.create()  # Much faster than step.create_db_objects()
```

---

## BaseJob Methods

Once you have a job object from `get_job()`, you can use these methods:

### Execution Methods

- **`job.run()`** - Execute the job (full pipeline: pre-run checks, invoke, data processing, post-run checks)
- **`job.for_each_run()`** - Run the job excluding invokers and checks (just data processing)
- **`job.overwrite()`** - Overwrite schema and run the job

### Schema Management

- **`job.create()`** - Create the database object (table/view) for the job
- **`job.update_schema()`** - Update table schema to match current data structure
- **`job.overwrite_schema()`** - Overwrite table schema completely
- **`job.register()`** - Register an existing table/view

### Data Operations

- **`job.truncate()`** - Truncate the table (remove all data)
- **`job.drop()`** - Drop the table/view

### Invokers and Checks

- **`job.invoke_pre_run()`** - Execute pre-run invokers (notebooks)
- **`job.invoke_post_run()`** - Execute post-run invokers (notebooks)
- **`job.check_pre_run()`** - Execute pre-run data quality checks
- **`job.check_post_run()`** - Execute post-run data quality checks

### Gold-Specific Methods

For Gold jobs, you may need to register UDFs first:

```python
from fabricks.api import get_job
from fabricks.core.jobs import Gold

job = get_job(job="gold.sales_monthly_summary")
if isinstance(job, Gold):
    job.register_udfs()  # Register UDFs before running
job.run()
```

### Job Properties

- **`job.step`** - Step name
- **`job.topic`** - Topic identifier
- **`job.item`** - Item identifier
- **`job.job_id`** - Hashed job ID
- **`job.expand`** - Expand type ("bronze", "silver", "gold")
- **`job.mode`** - Job mode (append, complete, update, etc.)
- **`job.table`** - Table object (for accessing table metadata)
- **`job.conf`** - Full job configuration

### Examples

```python
from fabricks.api import get_job

# Get and run a job
job = get_job(job="gold.sales_monthly_summary")
job.run()

# Create a new job's table
job = get_job(step="gold", topic="sales", item="new_report")
job.create()

# Update schema after adding columns
job = get_job(job="silver.sales_cleaned")
job.update_schema()

# Truncate and rerun
job = get_job(job="gold.sales_monthly_summary")
job.truncate()
job.run()

# Run only data processing (skip checks/invokers)
job = get_job(job="gold.sales_monthly_summary")
job.for_each_run()
```

---

## BaseStep Methods

**⚠️ Performance Warning**: Step-level operations process **all jobs in the step**, which can take significant time (minutes to hours depending on step size). **Always prefer job-level operations** (`get_job()`) for individual job tasks.

Use step-level methods only for:
- Initial deployment/setup
- Bulk operations across many jobs
- Updating step-level metadata

Once you have a step object from `get_step()`, you can use these methods:

### Deployment Methods

- **`step.create()`** - Create/update step (calls `step.update()`)
- **`step.update()`** - Update configurations and create missing database objects
- **`step.drop()`** - Drop step database and all related objects

### Configuration Methods

- **`step.update_configurations()`** - Update job configurations from YAML files
- **`step.create_db_objects()`** - Create tables/views for all jobs in the step
- **`step.update_dependencies()`** - Update job dependency relationships

### Metadata Methods

- **`step.update_tables_list()`** - Update the list of tables in `fabricks.{step}_tables`
- **`step.update_views_list()`** - Update the list of views in `fabricks.{step}_views`
- **`step.update_steps_list()`** - Update step metadata in `fabricks.steps`

### Data Retrieval Methods

- **`step.get_jobs(topic=None)`** - Get DataFrame of all jobs in the step
- **`step.get_dependencies(topic=None, include_manual=False)`** - Get dependencies DataFrame and errors

### Step Properties

- **`step.name`** - Step name
- **`step.expand`** - Expand type ("bronze", "silver", "gold")
- **`step.database`** - Database object for the step
- **`step.storage`** - Storage path for the step
- **`step.runtime`** - Runtime path for the step
- **`step.workers`** - Number of parallel workers
- **`step.timeouts`** - Timeout configuration (job, step)
- **`step.options`** - Step options from runtime config
- **`step.conf`** - Full step configuration

### Examples

```python
from fabricks.api import get_step

# Get and update a step
step = get_step("gold")
step.update()  # Updates configs and creates missing objects

# Update only configurations
step = get_step("silver")
step.update_configurations()

# Create missing database objects
step = get_step("gold")
step.create_db_objects()

# Update dependencies
step = get_step("gold")
df, errors = step.update_dependencies(progress_bar=True)

# Get all jobs in a step
step = get_step("gold")
jobs_df = step.get_jobs()
jobs_df.show()

# Get jobs for a specific topic
step = get_step("gold")
sales_jobs = step.get_jobs(topic="sales")

# Get dependencies
step = get_step("gold")
deps_df, errors = step.get_dependencies(topic="sales")
```

### Common Step Workflows

**⚠️ Important**: These workflows operate on **all jobs in the step** and can be time-consuming. Use only when necessary.

**Workflow 1: Add New Jobs to Step (Bulk)**
```python
from fabricks.api import get_step

# After adding multiple new YAML configs - use step-level for bulk
step = get_step("gold")
step.update()  # Reads new configs and creates missing objects for ALL jobs
```

**Preferred: Add Single Job**
```python
from fabricks.api import get_job

# For a single new job - much faster
job = get_job(step="gold", topic="sales", item="new_report")
job.create()  # Only creates this one job
```

**Workflow 2: Update Dependencies (Bulk)**
```python
from fabricks.api import get_step

# After modifying parent relationships across many jobs
step = get_step("gold")
step.update_dependencies(progress_bar=True)  # Updates ALL jobs in step
```

**Preferred: Update Single Job**
```python
from fabricks.api import get_job

# For a single job - dependencies are resolved automatically when job runs
job = get_job(job="gold.sales_monthly_summary")
job.run()  # Dependencies are checked automatically
```

**Workflow 3: Recreate Step (Full Reset)**
```python
from fabricks.api import get_step

# Drop and recreate everything - use only for full reset
step = get_step("gold")
step.drop()  # Drops ALL jobs in step
step.create()  # Recreates ALL jobs in step
```

**Preferred: Recreate Single Job**
```python
from fabricks.api import get_job

# For a single job - much faster
job = get_job(job="gold.sales_monthly_summary")
job.drop()  # Only drops this job
job.create()  # Only recreates this job
```

---

## Runtime Configuration (conf.fabricks.yml)

The main runtime YAML defines project-level settings, step defaults, and paths.

### Basic Structure

```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope          # Databricks secret scope
  timeouts:
    step: 3600                            # Step timeout (seconds)
    job: 3600                            # Job timeout (seconds)
    pre_run: 3600                        # Pre-run timeout
    post_run: 3600                       # Post-run timeout
  workers: 4                              # Parallel workers per step
  retention_days: 7                      # Table retention
  timezone: Europe/Paris                 # Timezone for timestamps

path_options:
  storage: abfss://fabricks@$standard    # Default storage path
  udfs: fabricks/udfs                    # UDFs directory
  extenders: fabricks/extenders          # Extenders directory
  parsers: fabricks/parsers              # Parsers directory
  schedules: fabricks/schedules          # Schedules directory
  views: fabricks/views                  # Views directory

spark_options:
  sql:
    spark.sql.parquet.compression.codec: zstd
    spark.sql.files.ignoreMissingFiles: true

# Step definitions
bronze:
  - name: bronze
    path_options:
      runtime: bronze                     # Runtime path for bronze jobs
      storage: abfss://bronze@$standard  # Storage path for bronze tables
    options:
      order: 101                          # Execution order

silver:
  - name: silver
    path_options:
      runtime: silver
      storage: abfss://silver@$standard
    options:
      order: 201
      parent: bronze                      # Parent step dependency

gold:
  - name: gold
    path_options:
      runtime: gold/gold
      storage: abfss://gold@$standard/gold
    options:
      order: 301
      timeouts:
        job: 1800                         # Override global job timeout
```

### Configuration Precedence

When the same setting appears in multiple places, precedence is (highest to lowest):
1. **Job-level options** in YAML (per job)
2. **Step-level defaults** in runtime YAML (per step instance)
3. **Global options** in runtime YAML (`options`, `spark_options`, `path_options`)
4. **Project-level defaults** in `pyproject.toml` `[tool.fabricks]`
5. **Internal defaults** (shipped with Fabricks)

---

## Steps: Bronze, Silver, Gold

### Step Overview

| Step | Purpose | Typical Modes | CDC Support |
|------|---------|---------------|-------------|
| **Bronze** | Raw data ingestion | `memory`, `append`, `register` | No |
| **Silver** | Data cleaning & validation | `memory`, `append`, `latest`, `update`, `combine` | SCD1, SCD2 |
| **Gold** | Business analytics | `memory`, `append`, `complete`, `update`, `invoke` | SCD1, SCD2 |

### Bronze Step

**Purpose**: Ingest raw data from external sources with minimal transformation.

**Common Modes**:
- `memory`: Creates a temporary view (no physical table)
- `append`: Appends data to a Delta table
- `register`: Registers an existing Delta table

**Preferred Pattern**: The recommended approach for Bronze is to use **`mode: register`** in combination with a **pre-run invoker** that stores a Delta file. This pattern provides:
- Better control over data ingestion logic
- Ability to handle complex data sources (APIs, databases, etc.)
- Separation of ingestion logic from Fabricks orchestration
- Easier testing and debugging of ingestion processes

**Example Job Configuration** (`bronze/_config.example.yml`):

**Preferred Pattern - Pre-run Invoker with Register**:
```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: register                          # Register existing Delta table
      uri: abfss://fabricks@$datahub/raw/sales/daily_transactions  # Delta file location
      keys: [transaction_id]                  # Primary keys
      source: pos_system                       # Source system identifier
    invoker_options:
      pre_run:                                # Pre-run invoker writes Delta file
        - notebook: bronze/invokers/ingest_sales
          arguments:
            source_url: https://api.example.com/sales
            target_path: abfss://fabricks@$datahub/raw/sales/daily_transactions
            date: "{{ today() }}"
```

The pre-run notebook (`bronze/invokers/ingest_sales`) would:
1. Fetch data from the source (API, database, files, etc.)
2. Transform/clean the data as needed
3. Write to a Delta table at the specified `uri`
4. Fabricks then registers this Delta table for use in downstream steps

**Alternative Patterns**:

**Pattern 1: Direct File Ingestion (Append)**:
```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: append
      uri: abfss://fabricks@$datahub/raw/sales  # Source data location
      parser: parquet                           # Parser type (parquet, json, etc.)
      keys: [transaction_id]                   # Primary keys
      source: pos_system                        # Source system identifier
```

**Pattern 2: API Call with Register**:
```yaml
- job:
    step: bronze
    topic: apidata
    item: http_api
    tags: [raw]
    options:
      mode: register
      uri: abfss://fabricks@$datahub/raw/http_api
      keys: [transaction_id]
    invoker_options:
      pre_run:
        - notebook: bronze/invokers/http_call
          arguments:
            url: https://api.example.com/data
            target_path: abfss://fabricks@$datahub/raw/http_api
```

**Key Bronze Options**:
- `mode`: `memory` | `append` | `register`
- `uri`: Source data URI (supports variables like `$datahub`)
  - For `register`: Path to existing Delta table
  - For `append`: Path to source files (parquet, json, etc.)
- `parser`: `parquet` | `json` | custom parser name (used with `append` mode)
- `keys`: List of primary key columns
- `source`: Source system identifier
- `invoker_options`: Optional pre-run/post-run notebooks
  - **Pre-run invokers**: Execute before data ingestion (recommended for `register` mode)
  - **Post-run invokers**: Execute after data registration

### Silver Step

**Purpose**: Clean, validate, deduplicate, and apply CDC patterns.

**Important**: Silver steps **do not use SQL files**. They automatically read from parent dependencies (Bronze or other Silver jobs) and apply CDC logic based on configuration. All transformations are handled declaratively through YAML configuration.

**Common Modes**:
- `memory`: Temporary view (reads from single parent)
- `append`: Append to table
- `latest`: Keep only latest version per key
- `update`: Merge/upsert data (used with CDC)
- `combine`: Combine multiple parent sources

**CDC Patterns**:
- `nocdc`: No change data capture
- `scd1`: Slowly Changing Dimension Type 1 (current state with `__is_current`, `__is_deleted`)
- `scd2`: Slowly Changing Dimension Type 2 (historical validity with `__valid_from`, `__valid_to`)

**Example Job Configuration** (`silver/_config.example.yml`):
```yaml
- job:
    step: silver
    topic: sales_analytics
    item: daily_summary
    tags: [processed, sales]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.daily_transactions]    # Parent job dependencies (required)
      filter_where: "status = 'active'"       # Optional: SQL WHERE clause to filter parent data
      extender: sales_extender                 # Optional: Python extender for transformations
      check_options:
        max_rows: 1000000                      # Data quality check
        min_rows: 10
```

**How Silver Works**:
1. Silver reads data from parent jobs (specified in `parents`)
2. Optionally applies `filter_where` to filter parent data
3. Optionally applies Python `extender` for custom transformations
4. Automatically applies CDC logic (SCD1/SCD2) based on `change_data_capture` setting
5. Writes to target table using the specified `mode`

**Key Silver Options**:
- `mode`: `memory` | `append` | `latest` | `update` | `combine`
- `change_data_capture`: `nocdc` | `scd1` | `scd2`
- `parents`: **Required** - List of parent jobs (e.g., `["bronze.daily_transactions"]`)
- `filter_where`: Optional SQL WHERE clause to filter parent data
- `extender`: Python extender function name for custom transformations
- `check_options`: Data quality checks (see below)

**CDC Columns**:
- **SCD1**: Adds `__is_current` (boolean), `__is_deleted` (boolean), creates `{table}__current` view
- **SCD2**: Adds `__valid_from`, `__valid_to` (timestamps) in addition to SCD1 columns

### Gold Step

**Purpose**: Create business-ready aggregated and transformed data for analytics using **SQL transformations**.

**Important**: Gold steps **require SQL files** (or notebooks via `invoke` mode). The SQL file name must match the job `item` (e.g., `monthly_summary.sql` for `item: monthly_summary`).

**Data Quality Best Practice**: **Heavy use of pre-run and post-run checks is strongly recommended** for Gold layer jobs. Since Gold produces business-critical analytics data, comprehensive data quality validation ensures:
- Input data quality before processing
- Output data quality after transformation
- Business rule validation
- Data completeness and accuracy
- Early detection of data issues before they reach consumers

**Common Modes**:
- `memory`: Temporary view
- `append`: Append to table
- `complete`: Full refresh (overwrite)
- `update`: Merge/upsert
- `invoke`: Execute a notebook instead of SQL

**Example Job Configuration** (`gold/gold/_config.example.yml`) - With comprehensive data quality checks:
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
      identity: true                           # Enable identity columns
      liquid_clustering: true                  # Enable liquid clustering
    check_options:
      min_rows: 1                               # Ensure at least 1 row
      max_rows: 1000000                        # Prevent data explosion
      pre_run: true                             # Run pre_run.sql before processing
      post_run: true                            # Run post_run.sql after processing
```

**Example SQL** (`gold/gold/monthly_summary.sql`):
```sql
select
    date_trunc('month', transaction_date) as month,
    sum(amount) as total_sales,
    count(*) as transaction_count
from
    silver.daily_summary
where
    __is_current = true
group by
    month
```

**SQL File Location**: 
- SQL files are placed in the step's runtime directory
- File name must match the job `item` (e.g., `item: monthly_summary` → `monthly_summary.sql`)
- Path: `{step_runtime}/{item}.sql` (e.g., `gold/gold/monthly_summary.sql`)

**Pre-run and Post-run Checks for Gold**:

Pre-run checks (`{item}.pre_run.sql`) should validate:
- Parent data availability and freshness
- Input data completeness (expected columns, non-null keys)
- Business rule validation on inputs
- Data volume expectations
- Referential integrity with other tables

Post-run checks (`{item}.post_run.sql`) should validate:
- Output data quality (completeness, accuracy)
- Business rule validation on outputs
- Aggregation correctness (sums, counts match expectations)
- Data consistency across related metrics
- Expected value ranges and distributions
- Duplicate detection
- Data freshness (timestamps, dates)

**Example Pre-run Check** (`gold/gold/monthly_summary.pre_run.sql`):
```sql
select
    case
        when (select count(*) from silver.daily_summary where __is_current = true) = 0
        then 'No current records in silver.daily_summary'
        when (select max(transaction_date) from silver.daily_summary where __is_current = true) < current_date() - interval 1 day
        then 'Silver data is stale (more than 1 day old)'
        when (select count(distinct date_trunc('month', transaction_date)) 
              from silver.daily_summary 
              where __is_current = true 
              and transaction_date >= date_trunc('month', current_date())) = 0
        then 'No data for current month'
        else null
    end as __message,
    case
        when __message is not null then 'fail'
        else 'warning'
    end as __action
```

**Example Post-run Check** (`gold/gold/monthly_summary.post_run.sql`):
```sql
select
    case
        when (select count(*) from gold.monthly_summary) = 0
        then 'No data in monthly_summary'
        when (select sum(total_sales) from gold.monthly_summary) < 0
        then 'Negative total_sales detected'
        when (select count(*) from gold.monthly_summary 
              where total_sales > 10000000) > 0
        then 'Unusually high sales values detected (possible data issue)'
        when (select count(*) from gold.monthly_summary 
              group by month 
              having count(*) > 1) > 0
        then 'Duplicate months detected'
        when (select max(month) from gold.monthly_summary) < date_trunc('month', current_date())
        then 'Latest month is not current month'
        else null
    end as __message,
    case
        when __message is not null then 'fail'
        else 'warning'
    end as __action
```

**Key Gold Options**:
- `mode`: `memory` | `append` | `complete` | `update` | `invoke`
- `change_data_capture`: `nocdc` | `scd1` | `scd2`
- `parents`: List of parent jobs
- `table_options`: Table properties (identity, clustering, etc.)
- `invoker_options`: Notebook invocation configuration (for `mode: invoke`)
- `check_options`: **Strongly recommended** - Data quality checks:
  - `pre_run: true` - Enable pre-run SQL checks
  - `post_run: true` - Enable post-run SQL checks
  - `min_rows`: Minimum expected row count
  - `max_rows`: Maximum expected row count
  - `count_must_equal`: Row count must equal another table

**SCD2 in Gold**: Gold can create SCD2 tables from base selects that include:
- `__key`: Unique key (any data type)
- `__timestamp`: Date when record changed/deleted
- `__operation`: `'upsert'` or `'delete'`

---

## Schedules

Schedules group jobs and define execution order across steps. They filter jobs by tags, steps, or custom views.

### Schedule Configuration

Schedules are defined in `fabricks/schedules/schedule.yml`:

```yaml
- schedule:
    name: test
    options:
      tag: test                              # Filter jobs by tag
      steps: [bronze, silver, transf, gold, semantic]  # Steps to execute in order
      variables:
        var1: 1                              # Schedule-level variables
        var2: 2
      view: monarch                          # Optional: Use a custom view to filter jobs
```

**Schedule Options**:
- `name`: Schedule identifier
- `tag`: Filter jobs by tag (jobs must have this tag)
- `steps`: List of steps to execute in order
- `view`: Optional SQL view name (from `fabricks/views/`) to filter jobs
- `variables`: Schedule-level variables available to jobs

### Custom Views for Schedules

Create SQL views in `fabricks/views/` to define job subsets:

**Example** (`fabricks/views/monarch.sql`):
```sql
-- Select all jobs with a specific topic
select *
from fabricks.jobs j
where j.topic = 'monarch'
```

Then reference in schedule:
```yaml
- schedule:
    name: run-monarch
    options:
      view: monarch                          # Uses fabricks.monarch view
      steps: [silver, gold]
```

### Running Schedules

Schedules are executed via Databricks notebooks or bundles:

**Notebook approach**:
```python
from fabricks.utils.helpers import run_notebook
from fabricks.context import PATH_RUNTIME

run_notebook(
    PATH_RUNTIME.parent().joinpath("schedule"),
    schedule="test"
)
```

**Schedule execution flow**:
1. `generate(schedule="test")` - Creates schedule metadata, resolves job list, builds dependency graph
2. Executes steps in parallel (each step runs its jobs)
3. `terminate(schedule_id=...)` - Finalizes schedule execution

### Schedule Views (Auto-generated)

Fabricks automatically creates views for each schedule:
- `fabricks.{schedule_name}_schedule` - Shows resolved job list for the schedule

Query to inspect:
```sql
select * from fabricks.test_schedule
```

---

## Job Configuration Details

### Job Structure

Each job is defined in a `_config.*.yml` file within the step's runtime directory:

```yaml
- job:
    step: gold                              # Step name (bronze/silver/gold)
    topic: sales                            # Topic identifier
    item: monthly_report                    # Item identifier
    tags: [report, sales]                   # Tags for filtering
    type: default                           # "default" | "manual" (manual jobs skip auto-execution)
    options:
      mode: complete                        # Write mode
      change_data_capture: scd2            # CDC pattern
      parents: [silver.daily_summary]       # Parent job dependencies
      filter_where: "status = 'active'"     # Optional: SQL WHERE clause filter
      optimize: true                        # Run OPTIMIZE after load
      compute_statistics: true              # Compute table statistics
      timeout: 1800                         # Job-specific timeout (seconds)
    table_options:
      identity: true                        # Enable identity columns
      liquid_clustering: true               # Enable liquid clustering
      properties:
        delta.minReaderVersion: 1
        delta.minWriterVersion: 7
    check_options:
      min_rows: 10                         # Minimum row count
      max_rows: 1000000                     # Maximum row count
      count_must_equal: fabricks.dummy      # Must equal another table's row count
      pre_run: true                         # Run pre_run.sql check
      post_run: true                        # Run post_run.sql check
    spark_options:
      sql:
        spark.databricks.delta.properties.defaults.targetFileSize: 1073741824
```

### Common Options Across All Steps

- `type`: `"default"` | `"manual"` - Manual jobs skip automatic execution
- `parents`: List of parent jobs (e.g., `["bronze.customers", "bronze.orders"]`)
- `filter_where`: SQL WHERE clause to filter source data
- `optimize`: Run OPTIMIZE command after load
- `compute_statistics`: Compute table statistics
- `vacuum`: Run VACUUM to remove old files
- `timeout`: Job-specific timeout (overrides step/global timeout)
- `tags`: List of tags for filtering in schedules

### Check Options (Data Quality)

Data quality checks can be configured per job. **Heavy use of checks is strongly recommended for Gold layer jobs** to ensure business-critical analytics data quality.

```yaml
check_options:
  min_rows: 10                              # Table must have at least N rows
  max_rows: 10000                           # Table must not exceed N rows
  count_must_equal: fabricks.dummy         # Row count must equal another table
  pre_run: true                             # Run pre_run.sql before load (strongly recommended for Gold)
  post_run: true                            # Run post_run.sql after load (strongly recommended for Gold)
```

**Pre-run and Post-run Checks**:
Create SQL files named `{table_name}.pre_run.sql` or `{table_name}.post_run.sql` that return:
- `__message`: Error/warning message
- `__action`: `'fail'` or `'warning'`

**Best Practice for Gold Layer**:
- **Pre-run checks**: Validate input data quality, freshness, completeness before processing
- **Post-run checks**: Validate output data quality, business rules, aggregation correctness after processing
- Use `'fail'` for critical issues that should stop the pipeline
- Use `'warning'` for non-critical issues that should be logged but allow processing to continue

Example (`gold/gold/monthly_summary.post_run.sql`):
```sql
select
    'Row count exceeds threshold' as __message,
    'fail' as __action
where
    (select count(*) from gold.monthly_summary) > 1000000
```

**Check Behavior**:
- `__action = 'fail'`: Job fails, non-zero exit, rollback to previous version (for physical tables)
- `__action = 'warning'`: Logs message, run continues
- For `memory` mode jobs: Errors are logged but no rollback occurs

---

## Common Patterns and Examples

### Pattern 1: Simple Bronze → Silver → Gold Pipeline

**Bronze** (`bronze/_config.sales.yml`) - Using preferred pattern:
```yaml
- job:
    step: bronze
    topic: sales
    item: transactions
    tags: [sales]
    options:
      mode: register                          # Preferred: register existing Delta table
      uri: abfss://fabricks@$datahub/raw/sales/transactions  # Delta table location
      keys: [transaction_id]
      source: pos_system
    invoker_options:
      pre_run:                                # Pre-run invoker writes Delta file
        - notebook: bronze/invokers/ingest_sales
          arguments:
            source_path: abfss://external@$datahub/incoming/sales
            target_path: abfss://fabricks@$datahub/raw/sales/transactions
            date: "{{ today() }}"
```

**Note**: The pre-run notebook handles data ingestion (reading from source, cleaning, writing Delta). Fabricks then registers the Delta table for downstream use.

**Silver** (`silver/_config.sales.yml`):
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
      filter_where: "status in ('completed', 'pending')"  # Optional: filter parent data
```

**Note**: Silver does not use SQL files. It automatically reads from `bronze.transactions` and applies CDC logic. Use `filter_where` for simple filtering, or `extender` for more complex transformations.

**Gold** (`gold/gold/_config.sales.yml`) - With data quality checks:
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
      min_rows: 1                               # Ensure data exists
      max_rows: 1000000                        # Prevent data explosion
      pre_run: true                             # Validate inputs before processing
      post_run: true                            # Validate outputs after processing
```

**Gold SQL** (`gold/gold/daily_summary.sql`):
```sql
select
    transaction_date,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    count(distinct customer_id) as unique_customers
from
    silver.transactions_cleaned
where
    __is_current = true
group by
    transaction_date
```

### Pattern 2: SCD2 Historical Tracking

**Silver with SCD2**:
```yaml
- job:
    step: silver
    topic: customers
    item: profile
    options:
      mode: update
      change_data_capture: scd2
      parents: [bronze.customers]
```

This creates columns: `__valid_from`, `__valid_to`, `__is_current`, `__is_deleted`, and a view `silver.customer_profile__current`.

### Pattern 3: Schedule with Custom View

**View** (`fabricks/views/nightly.sql`):
```sql
select *
from fabricks.jobs j
where j.topic in ('sales', 'inventory')
  and array_contains(j.tags, 'nightly')
```

**Schedule** (`fabricks/schedules/schedule.yml`):
```yaml
- schedule:
    name: nightly
    options:
      view: nightly
      steps: [bronze, silver, gold]
      variables:
        run_date: "{{ today() }}"
```

---

## Troubleshooting

### Common Issues

1. **"could not resolve runtime"**
   - Ensure `FABRICKS_RUNTIME` is set or `pyproject.toml` has `[tool.fabricks]` section
   - Check that the runtime path exists

2. **"conf mandatory"**
   - Ensure `conf.fabricks.yml` exists at the configured path
   - Check YAML syntax is valid

3. **Job dependencies not found**
   - Verify parent job names use correct format: `{step}.{topic}.{item}`
   - Check that parent jobs are defined in their step's `_config.*.yml`

4. **Schedule not finding jobs**
   - Verify jobs have the required `tag` if schedule uses `tag` filter
   - Check that jobs are in the steps listed in schedule `steps`
   - For custom views, ensure view returns `job_id` column

5. **CDC columns missing**
   - For SCD2, ensure SQL includes `__timestamp` column
   - For Gold SCD2, ensure SQL includes `__key`, `__timestamp`, `__operation`

### Debugging Tips

- Set `loglevel = "DEBUG"` in `pyproject.toml` or `FABRICKS_LOGLEVEL=DEBUG`
- Check `fabricks.jobs` table to see all registered jobs
- Query `fabricks.{schedule_name}_schedule` to see resolved schedule jobs
- Check `fabricks.last_schedule` for schedule execution history

---

## Key Files and Locations

- **Runtime config**: `fabricks/conf.fabricks.yml`
- **Schedules**: `fabricks/schedules/schedule.yml`
- **Views**: `fabricks/views/*.sql`
- **Job configs**: `{step}/_config.*.yml`
- **SQL files**: `{step}/*.sql` (Gold only - same name as job `item`)
- **UDFs**: `fabricks/udfs/`
- **Extenders**: `fabricks/extenders/`
- **Parsers**: `fabricks/parsers/`

---

## Quick Reference

### Job Naming Convention
- Format: `{step}.{topic}_{item}` (using separators)
- Example: `bronze.sales_data_daily_transactions`

### Table Naming Convention
- Format: `{step}.{topic}_{item}`
- Example: `bronze.sales_data_daily_transactions`

### Common Modes by Step
- **Bronze**: `memory`, `append`, `register`
- **Silver**: `memory`, `append`, `latest`, `update`, `combine` (no SQL files - reads from parents)
- **Gold**: `memory`, `append`, `complete`, `update`, `invoke` (requires SQL files or notebooks)

### CDC Patterns
- `nocdc`: No CDC columns
- `scd1`: Adds `__is_current`, `__is_deleted`, creates `{table}__current` view (in silver)
- `scd2`: Adds `__valid_from`, `__valid_to` in addition to SCD1

---

## Deploy Class

The `Deploy` class provides methods to deploy and manage Fabricks infrastructure components, including tables, views, UDFs, masks, notebooks, schedules, and steps. It's essential for setting up and maintaining your Fabricks environment.

### Import

```python
from fabricks.deploy import Deploy
```

### Methods

#### `Deploy.tables(drop: bool = False)`

Creates or replaces Fabricks system tables in the `fabricks` database:
- `fabricks.logs` - Execution logs for schedules, steps, and jobs
- `fabricks.steps` - Step metadata (step name, expand type, execution order)
- `fabricks.dummy` - Dummy table used for data quality checks (e.g., `count_must_equal`)

**Parameters**:
- `drop`: If `True`, drops existing tables before creating new ones

**Usage**:
```python
Deploy.tables()              # Create/replace tables
Deploy.tables(drop=True)     # Drop and recreate tables
```

#### `Deploy.views()`

Creates or replaces system views in the `fabricks` database. These views provide convenient access to job metadata, dependencies, and execution history:

- `fabricks.jobs` - Unified view of all jobs across all steps
- `fabricks.tables` - List of all tables created by jobs
- `fabricks.views` - List of all views created by jobs
- `fabricks.dependencies` - Job dependency relationships
- `fabricks.dependencies_flat` - Flattened dependency view
- `fabricks.dependencies_circular` - Detects circular dependencies
- `fabricks.last_schedule` - Most recent schedule execution
- `fabricks.last_status` - Latest execution status per job
- `fabricks.schedules` - All schedule definitions
- `fabricks.logs_pivot` - Pivoted log view for easier analysis

**Usage**:
```python
Deploy.views()
```

#### `Deploy.udfs(override: bool = True)`

Deploys User-Defined Functions (UDFs) from the runtime `udfs` directory:
- Registers all SQL UDFs found in `fabricks/udfs/`
- Creates `fabricks.udf_job_id()` function for job ID hashing

**Parameters**:
- `override`: If `True`, replaces existing UDFs with the same name

**Usage**:
```python
Deploy.udfs()                # Deploy UDFs, override existing
Deploy.udfs(override=False)   # Skip if UDF already exists
```

#### `Deploy.masks(override: bool = True)`

Deploys data masking functions from the runtime `masks` directory. Masks are used for data privacy and anonymization.

**Parameters**:
- `override`: If `True`, replaces existing masks with the same name

**Usage**:
```python
Deploy.masks()                # Deploy masks, override existing
Deploy.masks(override=False) # Skip if mask already exists
```

#### `Deploy.notebooks()`

Deploys helper notebooks to the Databricks workspace. These notebooks are copied from the Fabricks package to your configured `notebooks` path:

- `cluster.py` - Cluster management
- `initialize.py` - Initialization helper
- `process.py` - Step processing
- `schedule.py` - Schedule execution
- `run.py` - Job execution
- `terminate.py` - Schedule termination

**Usage**:
```python
Deploy.notebooks()
```

**Note**: This overwrites existing notebooks in the target directory.

#### `Deploy.schedules()`

Deploys schedule views and custom views:
- Creates schedule views (`fabricks.{schedule_name}_schedule`) for each schedule
- Creates custom views from SQL files in `fabricks/views/`

**Usage**:
```python
Deploy.schedules()
```

#### `Deploy.armageddon(steps, nowait: bool = False)`

**Full deployment and initialization** - This is the most comprehensive deployment method. It performs a complete reset and setup of your Fabricks environment.

**What it does**:
1. **Drops everything**:
   - Drops `fabricks` database
   - Drops all step databases (bronze, silver, gold, etc.)
   - Removes temporary storage folders (`tmp`, `checkpoints`, `schemas`, `schedules`)

2. **Recreates infrastructure**:
   - Creates `fabricks` database
   - Deploys system tables (`Deploy.tables(drop=True)`)
   - Deploys UDFs (`Deploy.udfs()`)
   - Deploys masks (`Deploy.masks()`)
   - Deploys notebooks (`Deploy.notebooks()`)

3. **Deploys steps**:
   - For each step, calls `BaseStep.create()` which:
     - Creates the step database
     - Updates job configurations from YAML files
     - Creates database objects (tables/views) for all jobs
     - Updates dependencies
     - Updates tables and views lists
     - Updates step metadata

4. **Finalizes**:
   - Deploys views (`Deploy.views()`)
   - Deploys schedules (`Deploy.schedules()`)

**Parameters**:
- `steps`: Optional. Can be:
  - `None` - Deploys all steps from runtime config
  - Single step: `"gold"` or `["gold"]`
  - Multiple steps: `["bronze", "silver", "gold"]`

**Usage**:
```python
# Deploy all steps
Deploy.armageddon(steps=None)

# Deploy specific steps
Deploy.armageddon(steps=["bronze", "silver", "gold"])
Deploy.armageddon(steps="gold")

# Skip warning delay
Deploy.armageddon(steps=None, nowait=True)
```

**Warning**: `armageddon` is destructive - it drops and recreates everything. Use with caution in production!

### Deployment Workflow

Typical deployment workflow:

1. **Initial Setup** (one-time):
   ```python
   from fabricks.deploy import Deploy
   
   # Full initialization
   Deploy.armageddon(steps=None)
   ```

2. **Update Individual Components**:
   ```python
   # After adding new UDFs
   Deploy.udfs()
   
   # After adding new views
   Deploy.views()
   
   # After updating schedules
   Deploy.schedules()
   ```

3. **Update Step** (after modifying job configs):

   **For a single job (preferred - much faster)**:
   ```python
   from fabricks.api import get_job
   
   job = get_job(step="gold", topic="sales", item="new_report")
   job.create()  # Only processes this one job
   ```

   **For multiple jobs (bulk - can be slow)**:
   ```python
   from fabricks.api import get_step
   
   step = get_step("gold")
   step.update()  # Processes ALL jobs in step - can take significant time
   ```

### Step Deployment Details

When `Deploy.armageddon()` or `BaseStep.create()` is called, it:

1. **Creates step database** (e.g., `gold`, `silver`, `bronze`)
2. **Updates configurations**: Reads YAML files and updates `fabricks.{step}_jobs` table
3. **Creates database objects**: For each job in the step:
   - Creates tables/views based on job `mode` and `type`
   - Applies table options (identity, clustering, properties)
   - Sets up CDC structures if configured
4. **Updates dependencies**: Resolves and stores parent-child relationships
5. **Updates metadata**: Updates `fabricks.{step}_tables`, `fabricks.{step}_views`, `fabricks.steps`

### Common Deployment Scenarios

**Scenario 1: New Project Setup**
```python
from fabricks.deploy import Deploy

# Complete initialization
Deploy.armageddon(steps=None)
```

**Scenario 2: Adding New Jobs to Existing Step**
```python
from fabricks.api import get_step

# Update step (reads new YAML configs and creates missing objects)
step = get_step("gold")
step.update()
```

**Scenario 3: Updating System Components**
```python
from fabricks.deploy import Deploy

# After modifying UDFs, views, or schedules
Deploy.udfs()
Deploy.views()
Deploy.schedules()
```

**Scenario 4: Recreating Everything (Clean Slate)**
```python
from fabricks.deploy import Deploy

# Nuclear option - drops and recreates everything
Deploy.armageddon(steps=None, nowait=True)
```

### Related Methods

The `BaseStep` class also provides deployment-related methods:

- `step.create()` - Creates/updates step (calls `step.update()`)
- `step.update()` - Updates configurations and creates missing database objects
- `step.drop()` - Drops step database and all related objects
- `step.update_configurations()` - Updates job configurations from YAML
- `step.create_db_objects()` - Creates tables/views for jobs
- `step.update_dependencies()` - Updates job dependency relationships

---

## Integration Test Examples

See `framework/tests/integration/runtime/` for comprehensive examples:
- Bronze: `bronze/_config.*.yml`
- Silver: `silver/_config.*.yml` (no SQL files)
- Gold: `gold/gold/_config.*.yml` + corresponding `*.sql` files
- Schedules: `fabricks/schedules/schedule.yml`
- Runtime config: `fabricks/conf.fabricks.yml`
