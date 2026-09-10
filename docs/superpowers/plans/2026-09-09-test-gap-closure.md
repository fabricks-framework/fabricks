# Test Coverage Gap Closure Plan

## Status

Implemented locally. Live Databricks verification remains pending. Test-only
work; no changes under `framework/fabricks/**`.

## Goal

Close the material coverage gaps between `main`'s broad Databricks integration
suite and the new plain/config/Apache/Databricks structure without restoring the
old stateful test matrix.

The existing iteration 1-11 scenarios remain CDC correctness tests. This plan
does not add another continuous 1-11 replay and does not describe those tests as
a long-lived process simulation.

## Decisions

- Keep production code fixed.
- Reuse committed Apache fixtures and expected states.
- Add only representative Gold wiring cases, not every CDC permutation.
- Add one small truncate/reload recovery case, not an iteration chain.
- Keep `semantic` as a separate named step in the runtime's `gold` family.
- Keep Unity Catalog-only behavior in the Databricks tier.
- Run each pytest tier in its own process.
- Treat a test exposing a production defect as a blocker unless a tracked,
  strict `xfail` is explicitly approved.

## Current Baseline

- Plain: 76 passing cases.
- Config: 126 collected cases.
- Apache: 54 passing cases.
- Databricks: 20 live-workspace cases.
- Apache runtime: approximately 6-11 minutes locally, depending on host state.
- CI currently runs only plain and config tiers.

## Task 1: Add Apache Spark to CI

### Files

- Modify `.github/workflows/python-test.yml`.

### Steps

1. Add an `apache` job separate from the existing unit-test job.
2. Use Ubuntu, Python 3.11, and Temurin Java 17.
3. Install dependencies with the lock file:

   ```bash
   uv sync --locked --group test --group dev
   ```

4. Cache uv and Ivy artifacts using OS, Java version, and
   `framework/uv.lock` in the cache key:

   ```text
   ~/.cache/uv
   ~/.ivy2.5.2
   ```

5. Do not cache `.storage`, `.worker_cwd`, Spark warehouses, or Derby state.
6. Set `SPARK_LOCAL_IP=127.0.0.1`.
7. Run the Apache tier with JUnit output:

   ```bash
   uv run pytest tests/spark/apache -v -ra --tb=long \
     --junitxml=artifacts/apache.xml
   ```

8. Set `timeout-minutes: 25`.
9. Upload JUnit, Derby, Spark, and JVM crash logs when the job fails.

### Acceptance

- Pull requests and pushes to `main` run Apache independently from unit tiers.
- Warm runs report uv and Ivy cache hits.
- A hung Spark test is terminated within 25 minutes with diagnostics retained.

## Task 2: Add Representative Gold-Through-CDC Tests

### Files

- Add `framework/tests/spark/apache/test_gold_cdc.py`.
- Add `framework/tests/spark/apache/runtime/gold/_config.cdc.yml`.
- Reuse `framework/tests/spark/test_data.py`.
- Reuse `framework/tests/spark/expected/compare.py`.

### Runtime jobs

Add four untagged jobs invoked directly by the tests:

| Job | Mode | CDC |
|---|---|---|
| `gold.cdc_scd0_update` | `update` | SCD0 |
| `gold.cdc_scd1_complete` | `complete` | SCD1 |
| `gold.cdc_scd2_update` | `update` | SCD2 |
| `gold.cdc_recovery` | `update` | SCD1 |

### Test matrix

1. **SCD0 update**
   - Feed iteration-1 and iteration-2 current snapshots.
   - Prove first-insert-wins through `Gold.for_each_batch()`.
   - Compare with the existing generated SCD0 oracle.
2. **SCD1 complete**
   - Feed iteration 1 once.
   - Prove Gold dispatches complete mode and materializes the expected state.
3. **SCD2 update**
   - Feed iteration 1.
   - Apply schema preparation for iteration 2.
   - Feed iteration 2 and compare with the existing SCD2 oracle.

Use the committed fixture adapters and derive the same `__key` shape consumed
by Gold. Do not introduce new expected snapshots.

### Acceptance

- Tests resolve jobs using `get_job(step="gold", ...)`.
- Empty-target and populated-target Gold paths are both exercised.
- Gold mode and CDC selection are tested without duplicating the CDC matrix.
- Added Apache runtime remains below approximately 90 seconds.

## Task 3: Add Focused Truncate/Reload Recovery

### Files

- Extend `framework/tests/spark/apache/test_gold_cdc.py`.
- Add only the SQL needed by
  `framework/tests/spark/apache/runtime/gold/cdc/recovery.sql`.

### Scenario

1. Materialize a tiny source containing IDs 1 and 2.
2. Run `gold.cdc_recovery` once.
3. Truncate the target and assert it is empty.
4. Replace the source with changed ID 1 and new ID 3.
5. Execute one `run(reload=True, invoke=False)`.
6. Assert IDs 1 and 3 are present with the expected current/deleted state and
   ID 2 is absent.

This proves the recovery control flow only. It does not replay fixture
iterations and is not a long-lived-process test.

### Acceptance

- Exactly one initial run and one reload occur.
- No iteration 1-11 chain is added or modified.
- The test fails if truncate, SQL reload, or Gold-to-CDC dispatch is broken.

## Task 4: Restore Semantic Execution in Apache

### Files

- Modify
  `framework/tests/spark/apache/runtime/fabricks/conf.fabricks.yml`.
- Modify `framework/tests/spark/apache/conftest.py` to create the `semantic`
  database.
- Add `framework/tests/spark/apache/runtime/semantic/fact/_config.semantic.yml`.
- Add self-contained SQL under
  `framework/tests/spark/apache/runtime/semantic/fact/`.
- Add `framework/tests/spark/apache/test_semantic.py`.

### Runtime registration

Register `semantic` as a second member of the runtime's `gold` family:

```yaml
gold:
  - name: gold
    # existing configuration
  - name: semantic
    path_options:
      storage: $apache_storage/semantic
      runtime: semantic
    options:
      order: 304
      metadata: true
      schema_drift: true
```

Tests resolve jobs using:

```python
get_job(step="semantic", topic="fact", item=...)
```

### Cases

1. Create a real semantic Delta table and assert rows and metadata behavior.
2. Prove semantic step properties are physically inherited.
3. Prove job properties physically override step properties.
4. Write multiple partition values and verify physical/catalog partitioning.
5. Write Zstandard-compressed data and verify Parquet footer metadata with
   PyArrow.
6. Verify portable Power BI Delta protocol and column-mapping properties.

Use self-contained `VALUES` SQL. Do not depend on the removed integration
fixture chain.

Keep masking and liquid clustering in Databricks because their execution
semantics are not portable to OSS Spark.

### Acceptance

- `semantic` remains a distinct named Gold-family step.
- Tests inspect materialized tables and files, not only generated options.
- No new top-level runtime family is introduced.
- Added runtime remains below approximately one minute.

## Task 5: Restore a Representative Databricks Schedule Failure Matrix

### Files

- Modify `framework/tests/spark/databricks/test_schedule.py`.
- Modify
  `framework/tests/spark/databricks/runtime/gold/gold/check/_config.check.yml`.
- Modify
  `framework/tests/spark/databricks/runtime/gold/gold/invoke/_config.invoke.yml`.
- Add `check/max_rows.sql`.
- Add `check/duplicate_key.sql`.
- Add `invoke/timeout.py`.
- Update `framework/tests/spark/databricks/runtime/README.md`.

### Schedule jobs

Retain the existing pre-run failure, skip, and warning. Add:

| Job | Mechanism |
|---|---|
| `gold.check_max_rows` | Three rows with `max_rows: 2` |
| `gold.check_duplicate_key` | Two rows sharing one `__key` |
| `gold.invoke_timeout` | 10-second timeout around a 20-second notebook |

Run the tagged schedule once. Do not create a parametrized schedule matrix.

### Assertions

- Exact failed set:
  - `gold.check_fail`
  - `gold.check_max_rows`
  - `gold.check_duplicate_key`
  - `gold.invoke_timeout`
- Exact skipped set: `gold.check_skip`.
- `gold.check_warning` is warned and completed, not failed.
- Failure text identifies pre-run, row-count, duplicate-key, and timeout
  causes respectively.
- Existing dependency-order, success, custom-view, and unexpected-failure
  assertions remain.

### Explicit exclusions

- No second Bronze streaming schedule case.
- No pre-run, post-run, and multi-notebook timeout variants.
- No additional schedule execution.

### Acceptance

- One schedule run proves all representative failure mechanisms.
- Added live runtime is approximately 10-30 seconds beyond notebook startup.
- Re-running after armageddon produces the same state.

## Task 6: Restore Databricks Type-Widening Execution

### Files

- Port and reduce the historical fixtures from
  `main:framework/tests/integration/runtime/gold/gold/type_widening/` into
  `framework/tests/spark/databricks/runtime/gold/gold/type_widening/`.
- Enable `options.type_widening: true` in the Databricks test runtime.
- Extend `framework/tests/spark/databricks/test_feature.py`.

### Cases

1. **Complete/overwrite widening**
   - Materialize an integer-typed column.
   - Replace it with a wider numeric type.
   - Assert physical schema and values.
2. **Merge/update widening**
   - Materialize an integer-typed Delta target.
   - Merge wider values.
   - Assert schema widening and preservation of existing rows.

Do not move these cases to Apache until OSS Delta behavior is proven. The
current Apache test documents that its widening attempt failed while the
production implementation suppressed the underlying error.

### Failure policy

- Do not modify production code in this work.
- A failure on the supported Databricks runtime blocks acceptance.
- A strict `xfail` requires a tracked issue and explicit approval.
- Never catch the widening failure and convert it into a passing assertion.

## Task 7: Documentation and Final Verification

### Files

- Update `docs/TEST.md`.
- Update `framework/tests/spark/databricks/runtime/README.md`.

### Document

- Apache CI ownership.
- Gold-through-CDC versus direct CDC coverage.
- The focused recovery test's limited purpose.
- `semantic` as a named Gold-family step.
- The live schedule's intentional failure set.
- Databricks-only type widening, masking, and liquid clustering.
- Explicit exclusion of a new continuous 1-11 test.

### Verification

Run each tier in its own process:

```bash
just test-plain
just test-config
just test-apache
databricks bundle deploy -t test
databricks bundle run fabricks_test_job -t test
```

Also verify:

```bash
uv run ruff check tests/
uv run ty check tests/
git diff --check
```

## Rollout

Land as separate commits:

1. `ci: run Apache Spark tests`
2. `test: cover Gold CDC wiring and recovery`
3. `test: restore semantic execution coverage`
4. `test: broaden Databricks schedule failures`
5. `test: restore Databricks type widening`
6. `docs: update test coverage ownership`

Every commit must leave existing tiers green. The final diff must contain no
changes under `framework/fabricks/**` and no additional continuous 1-11 test.

## Deferred Gaps

- Additional Bronze checkpoint/second-run behavior.
- The historical notebook-timeout position matrix.
- Broad restoration of weak "job completed" checks for every plugin and table
  maintenance variant.
- Step metadata repair/idempotency beyond current focused tests.
- Any production fix exposed by these tests.

## Post-Implementation Gap Audit

The full comparison against `main` found the following remaining test-only
work. These items are not part of the implementation above and remain planned.

### Priority 1: Strengthen newly added Gold proofs

- Replace the SCD2 source derived from expected target history with two small
  event batches where update and complete modes produce different results.
- Create the recovery target, then execute both the initial and reload paths
  through `job.run()`.
- Assert the complete unfiltered recovery table and CDC flags.
- Move semantic Zstandard configuration into runtime/job configuration rather
  than setting Spark configuration directly in the assertion.
- Assert representative populated semantic metadata fields, not column
  presence alone.

### Priority 2: Restore missing high-value behavior

- Add Gold NoCDC update coverage with an SCD2-shaped source.
- Persist Gold dependency edges and assert stable job/parent/dependency IDs,
  including notebook-derived dependencies.
- Add a dedicated runnable step for `Step.create_db_objects()` coverage.

### Priority 3: Restore focused physical assertions

- Persist and assert the Gold last-timestamp value.
- Verify semantic schema drift updates both the table and current view.
- Test step configuration repair/idempotency after deleted and stale rows.
- Add one known `__key`/`__hash` digest alongside invariant-based hashing tests.
- Add focused manual Gold mode and calculated-column execution checks.

### Verification still pending

- Run the new GitHub Apache workflow in Actions.
- Deploy and run the live Databricks schedule/type-widening suite.
- Confirm timeout exception text and supported-runtime widening behavior.

No new continuous iteration 1-11 test is planned; those scenarios remain CDC
algorithm coverage.
