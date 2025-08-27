# Getting Started

This quickstart guides you through running a minimal Fabricks pipeline end-to-end in minutes. It focuses on essentials: project setup, runtime layout, a schedule, and executing a simple job.

What you’ll do
- Configure Fabricks in your project (pyproject.toml)
- Create a minimal runtime with step folders and config
- Run a simple Gold job and inspect outputs
- Learn where to go next

Prerequisites
- Python 3.9+ and a working Databricks/Spark environment
- Access to a storage location (e.g., abfss:// or dbfs:/) if you plan to persist tables
- Recommended: mkdocs-material to preview docs (optional)

Install
Use your project’s environment. If you vendor Fabricks as a library:
```bash
pip install fabricks
```

If Fabricks is part of your mono-repo, ensure your environment resolves it (editable install or workspace).

1) Configure your project
Add Fabricks configuration to pyproject.toml:
```toml
[tool.fabricks]
runtime = "examples/runtime"                          # Path to your runtime (jobs, SQL, configs)
notebooks = "fabricks/api/notebooks"                  # Notebook helpers shipped with Fabricks (optional)
job_config_from_yaml = true                           # Enable YAML job config
loglevel = "info"
debugmode = false
config = "examples/runtime/fabricks/conf.fabricks.yml"  # Main runtime config
```
Key fields:
- runtime: points to your project’s Fabricks runtime (see layout below)
- config: path to the main runtime YAML (project options, step paths, defaults)
- job_config_from_yaml: when true, loads job definitions from step folders
- notebooks: optional helper notebooks path (used by invoke/notebook modes)

2) Create a minimal runtime
You can copy the structure below. This mirrors the integration-test sample, with a working Gold job.

Directory layout:
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

Minimal contents:

examples/runtime/fabricks/conf.fabricks.yml
```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope
  timeouts:
    step: 3600
    job: 3600
    pre_run: 3600
    post_run: 3600
  workers: 2
path_options:
  storage: /mnt/data  # adjust to your env (dbfs:/mnt/... or abfss://...)
spark_options:
  sql:
    spark.sql.parquet.compression.codec: zstd

bronze:
  - name: bronze
    path_options:
      runtime: bronze
      storage: /mnt/data/bronze

silver:
  - name: silver
    path_options:
      runtime: silver
      storage: /mnt/data/silver

gold:
  - name: gold
    path_options:
      runtime: gold
      storage: /mnt/data/gold
```

examples/runtime/fabricks/schedules/schedule.yml
```yaml
- schedule:
    name: example
    options:
      tag: example
      steps: [gold]
      variables: {}
```

examples/runtime/gold/gold/_config.example.yml
```yaml
- job:
    step: gold
    topic: demo
    item: hello_world
    options:
      mode: complete
```

examples/runtime/gold/gold/hello_world.sql
```sql
select 1 as id, 'hello' as monarch, 1.0 as value
```

3) Run the sample
Fabricks can run via Databricks bundle jobs or notebooks depending on your deployment. Typical paths:

A) Databricks job (bundle)
- Configure your bundle to pass the schedule name: example
- Trigger the job from your CI/CD or Databricks UI

B) Notebook helpers
- Use the shipped notebooks in notebooks = "fabricks/api/notebooks" to invoke a run with schedule: example

On successful execution, expect:
- A materialized Gold table (or view) for demo.hello_world (location and format depend on your environment)
- Logs indicating step/job completion
- If using complete mode, table is fully refreshed

4) Verify outputs
Run simple SQL in your environment:
```sql
select * from gold.demo_hello_world
```
Or if running in memory mode:
```sql
select * from gold.demo_hello_world__view
```
(Note: memory mode produces views only and ignores columns starting with __)

Next steps
- Build your first pipeline (Bronze → Silver → Gold): Tutorials → Your First Pipeline
- Understand runtime structure and schedules: Runtime
- Learn about CDC and SCD1/SCD2 patterns: Change Data Capture (CDC)
- Add checks and contracts for data quality: Checks & Data Quality
- Apply table properties and clustering: Table Options
- Extend with UDFs, Parsers, and Python Extenders: Extenders, UDFs & Parsers

Related
- Runtime: ../helpers/runtime.md
- Steps: Bronze • Silver • Gold • Semantic
- CDC: ../reference/cdc.md
- DQ Checks: ../reference/checks-data-quality.md
- Table Options: ../reference/table-options.md
- Extenders/UDFs/Parsers: ../reference/extenders-udfs-parsers.md
- Views: ../reference/views.md
