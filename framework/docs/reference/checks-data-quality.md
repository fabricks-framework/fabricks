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

## Catalog of checks

These are the built-in switches you can use in `check_options`. Built-in checks evaluate simple properties of the dataset and will fail the job on violation. Use SQL contracts when you need richer logic or to emit warnings instead of hard failures.

| Name              | Type                | Default | Required | Description                                                  | Failure behavior                                      |
|-------------------|---------------------|---------|----------|--------------------------------------------------------------|-------------------------------------------------------|
| pre_run           | boolean             | false   | no       | Run SQL contracts and built-in checks before the write.      | Contract: `__action` controls outcome.                |
| post_run          | boolean             | false   | no       | Run SQL contracts and built-in checks after the write.       | Contract: `__action` controls outcome.                |
| min_rows          | integer             | unset   | no       | Minimum expected row count of the dataset being written.     | Fail if `count < min_rows`.                           |
| max_rows          | integer             | unset   | no       | Maximum allowed row count of the dataset being written.      | Fail if `count > max_rows`.                           |
| count_must_equal  | string (table/view) | unset   | no       | Require row count to equal a reference table or view.        | Fail if counts differ.                                |
| skip              | boolean             | false   | no       | Disable all checks for this job.                             | No checks executed.                                   |

### Examples

Bounds only:
```yaml
check_options:
  pre_run: true
  min_rows: 10
  max_rows: 100000
```

Equality against a reference:
```yaml
check_options:
  post_run: true
  count_must_equal: analytics.dim_customer
```

Mixing built-ins with SQL contracts:
- Use `pre_run`/`post_run` to enable contract execution and add your contract SQL files (see “SQL contracts” below).
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

## CI integration

Checks produce two observable signals that are useful in CI and orchestration systems:

- Exit code
  - Any hard failure (built-in bound violations or a contract returning `__action = 'fail'`) causes the job to exit with a non‑zero status.
  - For physical tables (append/complete/update modes), failures also trigger an automatic rollback to the previous successful version.
  - Most CI systems will mark the pipeline as failed automatically on non‑zero exit codes.

- Log messages
  - Contracts should emit a single row with `__action` and `__message`. These are logged so you can surface them in CI logs and artifacts.
  - When `__action = 'warning'`, the job continues and exits with code 0. Ensure your CI surfaces logs so warnings are visible in pull requests.

Example contract outputs (see “SQL contracts”):
```sql
-- Fails the job and rolls back (physical tables)
select 'fail'    as __action, 'Row count below threshold' as __message;

-- Logs a warning; job continues
select 'warning' as __action, 'Null rate above target'    as __message;
```

Tips:
- Prefer `post_run` for checks that must consider the final persisted state.
- Use built-in bounds for simple invariants; reserve contracts for multi-table logic and nuanced policies.
- If you need to gate on warnings, implement a small CI step that scans logs for known warning markers and decides policy for your repository.

## Examples

- Working scenarios are available in the sample runtime:
  - Browsable: [Sample runtime](../helpers/runtime.md#sample-runtime)
- Rich integration scenarios (repository layout):
  - `framework/tests/integration/runtime/gold/gold/`

## Related topics

- Steps: [Bronze](../steps/bronze.md) • [Silver](../steps/silver.md) • [Gold](../steps/gold.md)
- Table properties and physical layout: [Table Options](./table-options.md)
- Custom logic integration: [Extenders, UDFs & Parsers](./extenders-udfs-parsers.md)
