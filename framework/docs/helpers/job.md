# Job Helper

This helper describes a widget-driven approach to manage a Fabricks job. It focuses on lifecycle operations driven by the core job implementation: registering/creating a job, running it (with checks and invokers), schema evolution, and housekeeping.

The behaviors documented here are grounded in the framework core:

- [Generator](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)
- [Processor](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/processor.py)
- [Checker](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/checker.py)
- [Invoker](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/invoker.py)
- [Gold](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/gold.py)
- [factory](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/api/get_job.py)


## Typical usage

1) Resolve the job:
```python
from fabricks.api import get_job
j = get_job(job="gold.sales.orders")  # or pass step/topic/item explicitly
```

2) Run actions based on widgets:
```python
# Examples:
j.check_pre_run()
j.run(schedule="daily", invoke=True, reload=False)
j.check_post_run()
j.check_post_run_extra()

# Other common operations:
j.update_schema()
j.overwrite_schema()
j.register()
j.create()
# Cleanup if required:
# j.truncate()
# j.drop()
```

3) Gold jobs:
```python
# If your job is gold, ensure UDFs are registered before actions:
if getattr(j, "expand", None) == "gold":
    j.register_udfs()
j.run()
```

## Methods explained (from core)

Below is a summary of what each job-level method does in the Fabricks core, as implemented by the base classes. References indicate where behaviors are implemented.

- drop() — Generator.drop
    - Purpose: Remove all artifacts and CDC related to the job.
    - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.drop

- register() — Generator.register
    - Purpose: Register the job’s output object (table or view).
  - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.register

- create() — Generator.create
    - Purpose: Create the physical table or virtual view.
  - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.create, _create_table

- truncate() — Generator.truncate
    - Purpose: Clear data and artifacts without dropping the table definition.
  - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.truncate

- update-schema() — Generator.update_schema
    - Purpose: Evolve the table schema in-place.
  - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.update_schema, _update_schema

- overwrite-schema() — Generator.overwrite_schema
    - Purpose: Replace the table schema to match the computed DataFrame.
  - Reference: [fabricks/core/jobs/base/generator.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/generator.py)::Generator.overwrite_schema, _update_schema

- run(...) — Processor.run
    - Purpose: Orchestrate a full run with optional invocations, checks, and rollback semantics.
    - Distinct exceptions:
        - `SkipRunCheckWarning`, `PreRunCheckWarning`, `PostRunCheckWarning`
        - `PreRunInvokeException`, `PostRunInvokeException`
        - `PreRunCheckException`, `PostRunCheckException`
        - plus general failures resulting in rollback for persisted jobs
    - Reference: [fabricks/core/jobs/base/processor.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/processor.py)::Processor.run

- for-each-run(...) — Processor.for_each_run
    - Purpose: Execute the core per-run logic, stream-aware.
  - Reference: [fabricks/core/jobs/base/processor.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/processor.py)::Processor.for_each_run, _for_each_batch; error type: SchemaDriftError

- overwrite() — Processor.overwrite (abstract)
    - Purpose: Replace table/view contents with new results in one operation.
  - Reference: [fabricks/core/jobs/base/processor.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/processor.py)::Processor.overwrite

- pre-run-invoke() — Invoker.invoke_pre_run
    - Purpose: Execute configured invokers before the run.
    - Behavior:
        - Invokes both job-level and step-level invokers: `invoke_job(position="pre_run")`, `invoke_step(position="pre_run")`
        - Failures raise `PreRunInvokeException`
    - Reference: [fabricks/core/jobs/base/invoker.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/invoker.py)::{invoke_pre_run, invoke_job, invoke_step, invoke}

- post-run-invoke() — Invoker.invoke_post_run
    - Purpose: Execute configured invokers after the run.
    - Behavior:
        - Invokes job-level and step-level invokers with `position="post_run"`.
        - Failures raise `PostRunInvokeException`
  - Reference: [fabricks/core/jobs/base/invoker.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/invoker.py)::{invoke_post_run, invoke_job, invoke_step}

- pre-run-check() — Checker.check_pre_run
    - Purpose: Validate pre-conditions via SQL contracts.
    - Behavior:
        - If enabled in options, looks for `.pre_run.sql` in the runtime path and executes it.
        - Expects rows with columns `__action` in {'fail','warning'} and `__message`.
        - Failures raise `PreRunCheckException`; warnings raise `PreRunCheckWarning`.
    - Reference: [fabricks/core/jobs/base/checker.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/checker.py)::{check_pre_run, _check}

- post-run-check() — Checker.check_post_run (+ extras)
    - Purpose: Validate post-conditions via SQL contracts and row constraints.
    - Behavior:
        - Runs `.post_run.sql` if enabled and enforces fail/warning actions like pre-run.
        - Additionally runs `check_post_run_extra()` (min/max rows and count equality).
        - Failures raise `PostRunCheckException`; warnings raise `PostRunCheckWarning`.
  - Reference: [fabricks/core/jobs/base/checker.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/base/checker.py)::{check_post_run, check_post_run_extra}

- register_udfs() — Gold.register_udfs (only for Gold jobs)
    - Purpose: Register UDFs required by Gold transformations before running actions.
    - Reference: [fabricks/core/jobs/gold.py](https://github.com/fabricks-framework/fabricks/tree/main/framework/fabricks/core/jobs/gold.py) (class `Gold`)

## Notes and recommendations

- On persisted jobs, schema drift during a run can raise `SchemaDriftError`. Consider passing `reload=True` to update schema automatically when appropriate, or set `self.schema_drift=True` in configuration.
- Use `for_each_run` for per-batch/stream processing where your concrete job implements `for_each_batch`.
- Invokers and checks are optional and driven by configuration; enable or disable them from your job options.
- Be explicit with `invoke` and `reload` flags on `run` to control invocations and schema handling.

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Runtime overview and sample runtime: [Runtime](../runtime.md)
- Checks & Data Quality: [Checks and Data Quality](../reference/checks-data-quality.md)
- Table options and storage layout: [Table Options](../reference/table-options.md)
- Extenders, UDFs & Views: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
