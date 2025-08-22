# Step Helper (Databricks Notebook)

This helper describes a widget-driven approach in a Databricks notebook to manage a Fabricks step (bronze, silver, gold). It focuses on lifecycle operations driven by the core `BaseStep` implementation: creating/updating a step, discovering jobs, updating metadata (jobs/tables/views/dependencies), registering objects, and housekeeping.

The behaviors documented here are grounded in the framework core:

- [BaseStep](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)
- [factory](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/get_step.py)
- [timeouts](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/_types.py)

## Overview

At a high level, a step helper would:

  - Present widgets to select a step (bronze/silver/gold), actions, and optional filters (topics)
  - Optionally toggle update/cleanup behaviors and progress bars
  - Resolve the step via `fabricks.core.steps.get_step`
  - Execute selected actions in sequence
  - Report progress/errors

Core imports used:
```python
from fabricks.core.steps.get_step import get_step
from fabricks.context.log import DEFAULT_LOGGER
```

## Typical usage in a Databricks notebook

1) Resolve the step:
```python
from fabricks.core.steps.get_step import get_step
s = get_step("gold")  # or "bronze"/"silver"
```

2) Run actions based on widgets:
```python
# Examples:
s.update(update_dependencies=True, progress_bar=True)
s.register(update=True, drop=False)
s.update_dependencies(topic=["sales", "finance"], progress_bar=False)
s.create()
# Cleanup if required:
# s.drop()
```

3) To inspect jobs and dependencies:
```python
jobs_df = s.get_jobs()
deps_df, errors = s.get_dependencies(progress_bar=True, topic="sales")
```

## Methods explained (from core)

Below is a summary of what each step-level method does in the Fabricks core, as implemented by `BaseStep`. References indicate where behaviors are implemented.

- drop() — BaseStep.drop
    - Purpose: Remove all artifacts for a step and drop its database and metadata tables.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.drop

- create() — BaseStep.create
    - Purpose: Initialize the step by updating resources if the runtime exists.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.create

- update(update_dependencies=True, progress_bar=False) — BaseStep.update
    - Purpose: Synchronize all objects for the step (jobs, dependencies, tables, views).
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.update

- get_dependencies(progress_bar=False, topic=None) — BaseStep.get_dependencies
    - Purpose: Collect job dependencies across the step.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.get_dependencies

- get_jobs_iter(topic=None) — BaseStep.get_jobs_iter
    - Purpose: Iterate YAML job definitions for the step (raw).
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.get_jobs_iter

- get_jobs(topic=None) — BaseStep.get_jobs
    - Purpose: Load step jobs into a Spark DataFrame with schema, computed `job_id`, and de-duplication.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.get_jobs

- create_jobs(retry=True) — BaseStep.create_jobs
    - Purpose: Create missing jobs (tables/views) physically.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.create_jobs

- update_jobs(drop=False) — BaseStep.update_jobs
    - Purpose: Refresh the SCD1 metadata table for jobs.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.update_jobs

- update_tables() — BaseStep.update_tables
    - Purpose: Refresh the SCD1 metadata table for physical tables.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.update_tables

- update_views() — BaseStep.update_views
    - Purpose: Refresh the SCD1 metadata table for views.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.update_views

- update_dependencies(progress_bar=False, topic=None) — BaseStep.update_dependencies
    - Purpose: Refresh the SCD1 metadata table for dependencies.
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.update_dependencies

- register(update=False, drop=False) — BaseStep.register
    - Purpose: Ensure the step database exists and all job outputs are registered (typically views for virtual jobs or tables where required).
    - Reference: [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep.register

- Properties and configuration — BaseStep
    - workers
        - Resolved from step options or global runtime options; cached.
    - timeouts: `Timeouts(job: int, step: int)`
        - Resolved from step options or global runtime options; cached.
    - conf / options
        - Step configuration and its `options` loaded from global `STEPS` settings and runtime YAML structure.
    - __str__ → step name

References:

- [fabricks/core/steps/base.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/base.py)::BaseStep
- [fabricks/core/steps/get_step.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/get_step.py)::get_step
- [fabricks/core/steps/_types.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/steps/_types.py)::Timeouts

## Notes and recommendations

- Ensure your runtime directory structure for the step exists before calling `create()` or `update()`. If not, the helper will log a warning.
- Consider enabling progress bars only for development/debug runs; for production, keep logs terse.
- When filtering by `topic`, pass a string or a list of strings; the helper will scope the updates accordingly.
- On large repositories, job creation/registration is parallelized with a fixed default of 16 workers in the current implementation.

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Runtime overview and sample runtime: [Runtime](../runtime.md)
- Checks & Data Quality: [Checks and Data Quality](../reference/checks-data-quality.md)
- Table options and storage layout: [Table Options](../reference/table-options.md)
- Extenders, UDFs & Views: [Extenders, UDFs & Views](../reference/extenders-udfs-views.md)
