# Test Inventory — current tests and where they land under the new tiers

Companion reference to
[2026-09-03-local-get-job-get-step.md](2026-09-03-local-get-job-get-step.md).
That plan's appendix inventories every *job* declared in
`tests/spark/databricks/runtime/`; this document inventories every *test
function* across all four tiers, what each one actually verifies, and
which tier it belongs in once the migration this session started is
finished. Built from this session's line-by-line reading of every file
listed below — not a summary of file names, a read of their bodies.

## Legend (same as the plan's appendix)

- **CONFIG** — pure decision/mapping logic on a real job/step object with
  Spark faked out (`tests/spark/config/`) — no container, no Java.
- **LOCAL** — needs real Spark+Delta *data* (`tests/spark/apache/`, real podman
  container) — no cloud file, no notebook, no Databricks Runtime feature.
- **LOCAL\*** — same as LOCAL, but needs a `uri`/parser/`dbutils` rework
  first (swap cloud path for local fixture, or `dbutils.fs.ls` for
  `pathlib`).
- **DATABRICKS** — genuinely needs a real notebook, real Unity Catalog
  state, or real multi-job wall-clock ordering. Stays on the cluster.
- **DONE** appended to a tier = already built and passing this session.
  No suffix = identified, not yet built.

---

## Already-built tests (the fast tiers, as they exist today)

### `tests/plain/` (51 tests, no Spark at all)

| File | Tests | What it verifies |
|---|---|---|
| `test_environment.py` | 1 | Default environment resolves to `databricks` when unset |
| `test_generate_local_fixtures.py` | 18 (7 + `test_king_queen_jsonl_matches_concatenation_of_king_and_queen` parametrized across all 11 iterations) | Local fixture-generation script logic (king/queen row shape consistency) plus a drift guard tying `king_queen.jsonl` to `bronze_king.jsonl`+`bronze_queen.jsonl` |
| `test_git_path.py` | several | `GitPath` path-joining/resolution, no filesystem I/O |
| `test_local_file_share_path.py` | several | `LocalFileSharePath`/`resolve_fileshare_path`'s docker-vs-cloud branch |
| `test_read_yaml.py` | several | YAML-reading + variable substitution helper |
| `test_variable_substitution.py` | several | `RuntimeConf`'s variable-file-vs-inline precedence |
| `test_sql_dependencies.py` **(built this session)** | 3 | `get_tables()` (`fabricks/utils/sqlglot.py`) — pure `sqlglot` table-reference extraction, the mechanism behind `Gold._get_sql_dependencies()`. Asserts the exact same dependency set as `tests/spark/databricks/jobs/job1/test_dependency.py::test_gold_fact_dependency_sql`, against the real fixture SQL |

### `tests/spark/config/` (27 tests, real `fabricks.context`/`get_step`/`get_job`, Spark faked)

| File | Tests | What it verifies |
|---|---|---|
| `test_ddl_option_mapping.py` | 10 | `Table._create()`'s `table_options` → DDL mapping: `TBLPROPERTIES`, `CLUSTER BY` (explicit + `auto`), identity column, primary key, foreign key, column masks/comments, special-char → column-mapping default. Captured off a mocked `spark.sql()` call; column DDL uses a *real* (Spark-session-free) `StructType` schema, not a stub |
| `test_column_selection.py` | 6 | `Generator._get_partitioning_columns`/`_get_clustering_columns`: explicit `table_options` wins, else auto-detect from column-name/dtype conventions (skipping boolean-typed clustering candidates) |
| `test_checker.py` | 8 | `Checker.check_post_run_extra()`'s min/max-rows/count-must-equal comparison + exact error messages, and `_check_run_time()`'s before/after skip-window logic — the same jobs `test_check.py::test_gold_check_max_rows`/`min_rows`/`count_must_equal` exercise today, minus the container |
| `test_option_hierarchy.py` | 3 | `Generator._get_option_hierarchy()`: job-level option wins, else step-level, else default. Mirrors `test_semantic.py::test_semantic_fact_step_option`/`job_option` |

### `tests/spark/apache/` (real Spark+Delta, podman container)

| File | Tests | What it verifies |
|---|---|---|
| `test_config_loads.py` | 1 | Storage paths resolve to `LocalFileSharePath` under the docker environment |
| `jobs/job1/test_cdc.py` | 2 | `NoCDC` overwrite/append — ports `test_cdc.py::test_gold_nocdc_overwrite`/`append` |
| `jobs/job1/test_silver.py` | 26 (2 functions × 13 `_SCENARIOS`) | Silver SCD1/SCD2 merge correctness against `expected/` oracle SQL — ports the `king_and_queen` chain, seeded from prior jobs' *expected* state rather than replayed. Now covers more than iter1-9: `iter10` (queen has a no-op delete-only batch) and `iter11` (queen has no data at all that iteration, so `king_and_queen_built` skips it and carries iter10's rows forward untouched) plus two multi-batch chain scenarios (`[4..7]`, `[1..9]`) — the delete-log/entity-optional-merge handling the Stage-1 plan's "Out of scope" section flagged as needing new design work has since been built |
| `test_hashing.py` | 5 | `hash.sql.jinja`'s `add_key`/`add_hash` macros directly, via Jinja's own macro loader — stability across calls, a changed value changes the hash, extra columns don't affect it, field order is significant, reload/upsert-vs-delete folding. Built specifically because the `king_and_queen` chain's `assert_dfs_equal` excludes `__key`/`__hash` from its oracle schema, so nothing else actually checks them (see note on `test_hashing` in the databricks `test_silver.py` table below — the "implicitly covered" claim there was wrong) |

---

## `tests/spark/databricks/jobs/job1/` — function-level (the richest suite; job2-5 mostly repeat these job *types* at later iterations, condensed below)

### `test_dependency.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_fact_dependency_sql` | SQL-parsed dependency graph (`fabricks.gold_dependencies`) for a SQL-bodied job, before and after `update_dependencies()` | Split: table-extraction half is **CONFIG (done)** via `test_sql_dependencies.py`; the `job_id`/`parent_id`/`dependency_id` hash values and catalog-table persistence still need `Step.update_dependencies()` writing to real `fabricks.*` tables — **LOCAL**, not yet built (see plan's Out of Scope) |
| `test_gold_fact_dependency_notebook` | Same, for a notebook-bodied job (4 parents, incl. hash-value assertions) — doesn't execute the notebook, only records the dependency edge | Same split as above |

### `test_schedule.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_schedule` | A standalone notebook run is expected to fail | **DATABRICKS** — real notebook execution |
| `test_no_unforced_failure` | No job in `fabricks.last_schedule` failed except the deliberately-forced list | **DATABRICKS** — needs a real full schedule to have run |
| `test_forced_failures` | Exactly the forced-failure jobs did fail | **DATABRICKS** |
| `test_no_unforced_skip` | No job skipped except the deliberately-forced list | **DATABRICKS** |
| `test_forced_skips` | Exactly the forced-skip jobs did skip | **DATABRICKS** |

### `test_check.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_check_fail` | Check fails with exact message, table stays empty | **CONFIG\*** — same shape as the two below, needs `.pre_run.sql`/`.post_run.sql` file handling mocked too (not yet built) |
| `test_gold_check_warning` | Check warns, table still populated | **CONFIG\*** |
| `test_gold_check_max_rows` | `max_rows` comparison + message | **CONFIG (done)** — `test_checker.py` |
| `test_gold_check_min_rows` | `min_rows` comparison + message | **CONFIG (done)** |
| `test_gold_check_count_must_equal` | `count_must_equal` comparison + message | **CONFIG (done)** |
| `test_gold_check_skip` | Skip-check warns, table stays empty | **CONFIG\*** |
| `test_gold_check_no_dependency_fail` | *(skipped — fixture not implemented)* | n/a |
| `test_gold_check_duplicate_key` | *(skipped — fixture not implemented)* | n/a |
| `test_gold_check_duplicate_identity` | *(skipped — fixture not implemented)* | n/a |

Note: `_config.check.yml` also declares `gold.check_time_ok`/`time_ko` jobs, but no test function in this file currently asserts them — `test_checker.py::test_check_run_time_*` (built this session) is new coverage, not a migration of an existing test.

### `test_invoke.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_invoke_notebook` | Notebook invocation status is `done` | **DATABRICKS** — real notebook execution + real status log |
| `test_gold_invoke_failed_pre_run` | Pre-run notebook failure propagates | **DATABRICKS** |
| `test_gold_invoke_post_run` | Post-run notebook status is `done` | **DATABRICKS** |

### `test_overwrite.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_dim_overwrite` | `.overwrite()` produces 2 rows, `__identity` column present, `identityColumns` table feature set | **LOCAL** — needs a real overwrite + real row/feature check; the identity-column *DDL* half is already **CONFIG (done)** via `test_ddl_option_mapping.py::test_create_ddl_includes_identity_column` |
| `test_gold_fact_overwrite` | `.overwrite()` produces 2 rows | **LOCAL** |

### `test_semantic.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_semantic_fact_table` | Table exists, has exactly one `__metadata` dunder column | **LOCAL** |
| `test_semantic_fact_step_option` | Step-level table properties (`minReaderVersion`=1, etc.) | Resolution logic **CONFIG (done)** via `test_option_hierarchy.py`; the real materialized-table property check is **LOCAL** |
| `test_semantic_fact_job_option` | Job-level table properties override step-level | Same split |
| `test_semantic_fact_zstd` | Parquet file compression codec, via `dbutils.fs.ls` | **LOCAL\*** — swap `dbutils.fs.ls` for `pathlib` against local storage |
| `test_semantic_fact_powerbi` | Table properties + partition column | **LOCAL** |

### `test_transf.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_transf_fact_wait_for` | Declared dependencies (`silver.monarch_scd1`, `transf.fact_memory`) + real wall-clock ordering vs. `transf.fact_memory`'s actual run | Split: the *declared* `wait_for`/parent dependency shape is a **CONFIG** candidate (same shape as `test_option_hierarchy.py`, not yet built); the real-ordering assertion is **DATABRICKS**, no way around it |

### `test_cdc.py`

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_nocdc_overwrite` | `NoCDC.overwrite()` idempotent row count | **LOCAL (done)** — ported to `tests/spark/apache/jobs/job1/test_cdc.py` |
| `test_gold_nocdc_append` | `NoCDC.append()` accumulates rows | **LOCAL (done)** |

### `test_gold.py` (18 tests)

| Test | What it verifies | Target |
|---|---|---|
| `test_gold_scd1_complete`/`update`/`identity` | SCD1 merge vs. `expected/` oracle | **LOCAL** — not yet ported (only `king_and_queen` silver chain is; this plan's Task 5/follow-on would extend the same pattern to gold) |
| `test_gold_scd2_complete`/`update` | SCD2 merge vs. oracle | **LOCAL** |
| `test_gold_scd0_update` | SCD0 merge vs. oracle | **LOCAL** |
| `test_gold_scd2_correct_valid_from` | `__valid_from` sentinel-date correction | **LOCAL** |
| `test_gold_fact_udf` | Registered UDF produces correct value | **LOCAL** |
| `test_gold_fact_memory` | Memory-mode view excludes an internal column | **LOCAL** |
| `test_gold_fact_order_duplicate` | Dedup-by-order-column produces 1 row with the latest value | **LOCAL** |
| `test_gold_fact_deduplicate` | Dedup produces 1 row | **LOCAL** |
| `test_gold_fact_manual` | Manual-type job's table stays empty | **LOCAL** |
| `test_gold_dim_identity`/`dim_date` | Identity column present/absent per job config | DDL half **CONFIG (done)**; real-table check **LOCAL** |
| `test_gold_fact_option` | Table properties/comment/cluster/spark-options/timeout all correct | DDL half **CONFIG (done)** via `test_ddl_option_mapping.py`; real-table check **LOCAL** |
| `test_gold_scd1_last_timestamp` | Persisted last-timestamp table is `None` on first run | **LOCAL** |
| `test_gold_fact_no_drop` | `.drop()` raises `ValueError` when `no_drop` is set | **CONFIG** candidate — pure option check + exception, not yet built |
| `test_gold_fact_masker_and_commenter` | Masked column values in real data | DDL half **CONFIG (done)**; real masked-value check is **LOCAL** (masking is a real Delta column-mask feature, needs actual query execution) |

### `test_silver.py` (17 tests)

| Test | What it verifies | Target |
|---|---|---|
| `test_silver_king_and_queen_scd2`/`scd1` | SCD1/SCD2 merge vs. oracle | **LOCAL (done)** — ported |
| `test_silver_monarch_scd2`/`scd1`, `regent_scd2`/`scd1`, `memory_scd2`/`scd1` | Same pattern, different topics | **LOCAL\*** — needs those topics' bronze sources seeded the same way `king`/`queen` were (register-mode substitute); not yet ported |
| `test_silver_monarch_delta` | SCD2 merge + a widened column's data type + an extended column present | **LOCAL\*** |
| `test_silver_princess_append`/`latest` | Row counts for append/latest modes | **LOCAL\*** |
| `test_silver_prince_special_char` | Special-character column names preserved | **LOCAL\*** |
| `test_silver_princess_extend` | Extender-added column present | **LOCAL\*** |
| `test_silver_princess_order_duplicate`/`calculated_column` | Dedup-by-order / calculated-column value | **LOCAL\*** |
| `test_silver_timeout` | Job timeout resolves to the step's configured value | **CONFIG** candidate (pure option resolution, same shape as `test_option_hierarchy.py`), not yet built |
| `test_hashing` | `__key`/`__hash` column values match exact expected hashes | **LOCAL (done)** — ported as `tests/spark/apache/test_hashing.py`, targeting `hash.sql.jinja`'s macros directly. Not "implicitly covered" by the `king_and_queen` chain as originally thought here: `assert_dfs_equal`'s oracle schema excludes `__key`/`__hash`, so that chain never actually compared them |

---

## `tests/spark/databricks/jobs/job2/`, `job3/`, `job4/`, `job5/` — condensed

These largely re-run the **same job types** as job1 at later chain iterations (`iter=2`, `iter=11`) or drive real orchestration directly. Per-test verdicts follow the same rules as their job1 counterparts above; only what's structurally different is called out.

| File | What it does | Target |
|---|---|---|
| `job2/test_gold.py`, `job2/test_silver.py` | SCD0/1/2 chain at `iter=2`, plus `test_gold_type_widening_overwrite`/`merge` (schema-widened column type), `test_silver_princess_schema_drift` (new column appears), `test_silver_princess_check` (commit-file bookkeeping) | **LOCAL\*** — same chain-extension pattern as `test_column_selection.py`'s reasoning; extend the existing parametrized `king_and_queen` local test's scenario list rather than porting a separate file |
| `job2/test_semantic.py` | `test_semantic_fact_schema_drift` — new column appears in a semantic table | **LOCAL** |
| `job2/test_run.py`, `job3/test_run.py`, `job4/test_run.py` | Literally drive `run_notebook`/`run_in_parallel` — this *is* the orchestration exercise | **DATABRICKS** — this is exactly the "simple schedule" the suite should keep |
| `job3/test_silver.py` | Same chain at `iter=11`, plus `job.truncate(); job.run()` reload idempotency check via `get_job()` | **LOCAL\*** for the merge-correctness part; the `truncate()+run()` reload-through-`get_job()` path is the one piece flagged in the plan's Out of Scope as needing the job/config layer this session didn't build |
| `job4/test_gold.py` | Same chain at `iter=11` | **LOCAL\*** |
| `job5/test_step.py` | `Step._get_dependencies_internal()`, `update_dependencies()`, `create_db_objects()`, `update_configurations()` | Dependency-aggregation logic (no catalog writes) is a **CONFIG** candidate (flagged in the plan's Out of Scope, not yet built); the catalog-table-writing parts (`fabricks.gold_jobs`/`fabricks.transf_views`) need real Spark — **LOCAL**, needs the `fabricks` database created in `tests/spark/apache/conftest.py` first |

---

## Summary

| Tier | Built (tests) | Identified, not built | Genuinely can't move |
|---|---|---|---|
| `tests/plain/` | 51 (incl. `test_sql_dependencies.py`) | — | — |
| `tests/spark/config/` | 27 across 4 files | ~15-20 more (check-file variants, `fact_no_drop`, `silver_timeout`, `wait_for`'s static half, `Step._get_dependencies_internal`, `Gold.get_cdc_context`, `create_table()`'s default-properties selection) | — |
| `tests/spark/apache/` | 34 across 4 files | Most of `test_gold.py`/`test_silver.py`'s remaining merge-correctness tests, job2-4's chain extensions, `test_overwrite.py`/`test_semantic.py`'s real-table checks | — |
| `tests/spark/databricks/` | (unchanged) | — | Real notebook execution (`invoke_*`, `*_notebook`, `fact_sample`, `run.py`'s), real wall-clock ordering (`wait_for`, `test_schedule.py`), real Unity Catalog data checks not reducible to DDL |
