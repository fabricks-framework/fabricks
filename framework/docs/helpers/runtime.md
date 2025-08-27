# Runtime Configuration

The Fabricks runtime is the folder where your Lakehouse project lives (configs, SQL, jobs, UDFs, extenders, views). This page explains how to point Fabricks to your runtime, how to structure it, and how to configure schedules and step paths.

## Minimal setup and verification

This quick path gets a working runtime running end-to-end using the sample included with Fabricks.

1) Prepare pyproject configuration
- Ensure your `pyproject.toml` contains a `[tool.fabricks]` block pointing to the sample runtime:
  ```toml
  [tool.fabricks]
  runtime = "examples/runtime"
  notebooks = "fabricks/api/notebooks"
  job_config_from_yaml = true
  loglevel = "info"
  debugmode = false
  config = "examples/runtime/fabricks/conf.fabricks.yml"
  ```

2) Inspect the sample runtime
- Directory: `examples/runtime` (see structure in “Sample runtime” below).
- It contains a minimal schedule and a Gold `hello_world.sql` full-refresh job.

3) Run a schedule
- Use your Databricks bundle/job or orchestration to run the schedule named `example` from `examples/runtime/fabricks/schedules/schedule.yml`.
- You can also run step-by-step via the shipped notebooks referenced by `notebooks`.

Expected outputs
- Tables/views:
  - A Gold table for the `hello_world` job (full refresh).
  - If using memory mode jobs, temporary views are registered for downstream steps.
- Logs:
  - A completion line indicating job success; warnings/errors surfaced from checks/contracts if configured.
- Data quality (if enabled):
  - Built-in bound violations or contract `__action = 'fail'` causes a non‑zero exit and, for physical tables, an automatic rollback to the last successful version.
  - Contract `__action = 'warning'` logs the message and the run continues.

Tip: If your environment uses different storage locations or workspace setup, adjust `path_options` and `spark_options` in the runtime YAML before running.

## Pointing to your runtime

Configure Fabricks in your project's `pyproject.toml`:

```toml
[tool.fabricks]
runtime = "tests/integration/runtime"          # Path to your runtime (jobs, SQL, configs)
notebooks = "fabricks/api/notebooks"           # Notebook helpers shipped with Fabricks
job_config_from_yaml = true                     # Enable YAML job config
loglevel = "info"
debugmode = false
config = "tests/integration/runtime/fabricks/conf.uc.fabricks.yml"  # Main runtime config
```

- runtime: path to your project&#39;s runtime directory
- config: path to your main runtime YAML configuration (see below)
- notebooks: optional helpers used by shipped notebooks
- job_config_from_yaml: enable loading jobs from YAML files
- loglevel/debugmode: control logging verbosity

## Main runtime config (YAML)

Define project-level options, step defaults, and step path mappings in `fabricks/conf.fabricks.yml`:

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

Key concepts:
- options: global project config (secrets, timeouts, worker count)
- path_options: shared storage/config paths
- spark_options: default Spark SQL options applied for jobs
- step sections (bronze/silver/gold/...): list of step instances with their runtime and storage paths

## Configuration precedence

Multiple layers of configuration can influence a run. When the same setting appears in several places, the following order applies (top wins):

1. Job-level options in YAML (per job)
2. Step-level defaults in runtime YAML (per step instance under bronze/silver/gold/semantic)
3. Global options in runtime YAML (`options`, `spark_options`, `path_options`)
4. Project-level defaults in `pyproject.toml` `[tool.fabricks]`
5. Internal defaults (shipped with Fabricks)

Notes
- `spark_options` are merged; job-level `spark_options` override keys from step/global levels.
- `path_options` (e.g., `storage`, `runtime`) can be set globally, then refined per step instance.
- Some options only make sense at specific levels (e.g., `check_options` at job level).
- For CDC and write behavior, job `options` (e.g., `mode`, `change_data_capture`) take precedence over step defaults.

## Schedules

Schedules group jobs and define step order. Place schedules in your runtime (commonly under `fabricks/schedules/`):

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

Pass the schedule name when running the shipped notebooks or the Databricks bundle job.

## Typical runtime structure

A minimal runtime can look like:

```
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

- bronze/silver/gold/semantic: step folders with YAML configs and SQL
- fabricks/conf.fabricks.yml: main runtime config for paths and defaults
- fabricks/schedules/: defines one or more schedules for execution

## Sample runtime

This section contains a minimal, copyable Fabricks runtime you can use to get started quickly. It mirrors the structure used in the integration tests and includes example configs and a working Gold job (`hello_world.sql`).

What this is
- A self-contained runtime layout showing how to organize steps (bronze, silver, gold, semantic)
- Example YAML configuration files per step
- A minimal schedule and config to run a simple job end-to-end

Directory overview
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

Key files and purpose
- `fabricks/conf.fabricks.yml`: project-level configuration (secret scope, timeouts, workers, storage paths, schedules path)
- `fabricks/schedules/schedule.yml`: minimal schedule to run the gold step
- `gold/gold/_config.example.yml`: defines a simple Gold job
- `gold/gold/hello_world.sql`: example SQL for a Gold job (full refresh mode)
- `bronze/_config.example.yml`, `silver/_config.example.yml`, `semantic/_config.example.yml`: example step configurations

How to use this sample
1) Point `tool.fabricks.runtime` to this folder and set `config` to `fabricks/conf.fabricks.yml` in your `pyproject.toml`.
2) Run the Databricks bundle job with `schedule: example` (see the main documentation for bundle details).
3) Inspect the materialized outputs, logs, and table/view artifacts.

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Data quality checks and contracts: [Checks & Data Quality](../reference/checks-data-quality.md)
- Table properties and physical layout: [Table Options](../reference/table-options.md)
- Custom logic integration: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
