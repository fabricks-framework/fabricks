# Sample Runtime

This directory contains a minimal, copyable Fabricks runtime you can use to get started quickly. It mirrors the structure used in the integration tests and includes example configs and a working Gold job (`hello_world.sql`).

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

Related docs
- See the main guide section “Sample runtime” for more context.
- Step references:
  - [Bronze](../../steps/bronze.md)
  - [Silver](../../steps/silver.md)
  - [Gold](../../steps/gold.md)
  - [Semantic](../../steps/semantic.md)
