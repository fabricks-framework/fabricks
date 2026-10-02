# Debugging

New entries follow this template: a `##` heading naming the failure, then
**Symptom**, **Cause**, **Workaround**.

## DBR 18 `foreachBatch` Isolated Worker Startup

**Symptom:** A streaming query fails from `.start()` before its batch callback
logs, with `ISOLATION_STARTUP_FAILURE.GENERIC` and
`SandboxClientTimeoutTracker(Some(60000 milliseconds))`.

**Cause:** On `USER_ISOLATION` compute, Databricks SafeSpark times out while
starting the isolated Python worker. This platform timeout is unrelated to the
framework query timeout.

**Workaround:** `write_stream()` retries this startup failure with backoff. A
streaming callback must serialize only job identity and configuration, then
reconstruct the job inside the worker instead of capturing the driver-side
`Processor`.

## `update_schema(widen_types=True)` Leaves the Column Type Unchanged

**Symptom:** The schema diff is detected and logged (`changed column value
(bigint -> double)`), but the column keeps its old type. No error is raised,
even with `delta.enableTypeWidening` set in `TBLPROPERTIES` at `CREATE TABLE`
or by a follow-up `ALTER TABLE ... SET TBLPROPERTIES`.

**Cause:** Not root-caused. `Table.update_schema(widen_types=True)` calls
`change_column()`, whose `ALTER TABLE ... CHANGE COLUMN ... TYPE` sits inside a
bare `except Exception: pass`, so the real error is swallowed. Three attempts to
cover it with an Apache-tier test failed. Suspected next step: the Delta
`minReaderVersion`/`minWriterVersion` negotiation, not only the boolean
property.

**Workaround:** None known. Start by logging the exception that
`change_column()` swallows. Widening is exercised on a live workspace by the
Databricks tier's `gold.type_widening_*` tests.

## `QUALIFY` Fails on Open-Source Spark

**Symptom:** `PARSE_SYNTAX_ERROR` on a query using `... qualify row_number()
over (...) = 1`, or a view built from a transpiled `select * ... qualify` that
has an extra window-helper column.

**Cause:** `QUALIFY` is a Databricks SQL extension. OSS Spark parses
`select * except (...)` but not `qualify`. sqlglot's Databricks-to-Spark
transpile rewrites it correctly for an explicit column list, but for a
`select *` projection it leaves its own helper column in the outer `select *`.
`fix_sql` only transpiles to the Databricks dialect, so NoCDC's dedup `qualify`
CTE runs on Databricks only.

**Workaround:** Apache-tier tests use SCD1/SCD2/SCD0, whose dedup takes the
plain `row_number()` branch (`deduplicate_key.sql.jinja`). The expected-state
oracle rewrites its `qualify` files at load time (`make_spark_compatible` in
`tests/support/expected_sql.py`, which handles only that one query shape).

## `BROADCAST` Hint Ignored in the Rectify CTE

**Symptom:** The `BROADCAST` hints in `rectify.sql.jinja` have no effect and the
join falls back to a shuffle (issue #203).

**Cause:** A sibling CTE (`ctes/current.sql.jinja`) aliases a table as `t`. A
same-named alias in the rectify join silently defeated hint resolution even
though the two sit in different scopes.

**Workaround:** The rectify join aliases are `__rectify_t` / `__rectify_nxt`.
Keep them distinct from any alias used elsewhere in the same query.

## Memory-Mode Views Race During Object Creation

**Symptom:** Creating a batch of views that reference each other fails for the
item attempted before its dependency is registered (issue #183).

**Cause:** Items dispatched together are not ordered by dependency.
`Job.get_dependencies()` would give the real graph, but it needs an
`explain extended` per notebook-backed job on every run, for a rare race.

**Workaround:** The retry loop in `core/steps/base.py` reads the missing
table/view from each error message and retries the blocked items in later
passes (a chain of depth N resolves in N passes). Items ready in the same pass
still run in parallel. Table and view lists are updated after the retries.
