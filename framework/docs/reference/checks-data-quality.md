# Checks and Data Quality

Fabricks provides built-in mechanisms to validate data before and/or after a job runs. Checks can enforce row counts, equality constraints, and custom SQL contracts that return actions and messages.

## Configuring checks

Enable checks per job using `check_options`:

```yaml
- job:
    step: gold
    topic: check
    item: max_rows
    options: { mode: complete }
    check_options:
      pre_run: true         # run checks before writing
      post_run: true        # run checks after writing
      min_rows: 10          # minimum expected row count
      max_rows: 10000       # maximum expected row count
      count_must_equal: fabricks.dummy  # enforce row count equality with another table
      skip: false           # skip all checks if true
```

Supported options:
- pre_run/post_run: Run checks around the execution of the job.
- min_rows/max_rows: Row count bounds.
- count_must_equal: Assert row count equals a reference table.
- skip: Disable checks for this job.

## SQL contracts

When `pre_run` or `post_run` checks are enabled, provide SQL files named after the table or job:
- `table_name.pre_run.sql`
- `table_name.post_run.sql`

Each SQL must return:
- `__action`: either `'fail'` or `'warning'`
- `__message`: human-readable message

Example:

```sql
-- fail.pre_run.sql
select "fail" as __action, "Please don't fail on me :(" as __message;

-- warning.post_run.sql
select "warning" as __action, "I want you to warn me !" as __message;
```

## Behavior and rollback

- For physical tables (append/complete/update modes), a failed check triggers an automatic rollback to the previous successful version.
- For `memory` mode (views), results are not persisted and failures are logged only.

## Examples

- Working scenarios are available in the sample runtime:
  - Browsable: [Sample runtime](../runtime.md#sample-runtime)
- Rich integration scenarios (repository layout):
  - `framework/tests/integration/runtime/gold/gold/`

## Related topics

- Step references:
  - [Bronze](../steps/bronze.md)
  - [Silver](../steps/silver.md)
  - [Gold](../steps/gold.md)
  - [Semantic](../steps/semantic.md)
- Table properties and physical layout: [Table Options](./table-options.md)
- Custom logic integration: [Extenders, UDFs & Views](./extenders-udfs-views.md)
