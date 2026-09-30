# Test Suite Consolidation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fewer, clearer tests and a cleaner tier layout, without losing failure signal.

**Origin:** Review of `framework/tests/` on 2026-09-30 using `testing-python-libraries`. Findings come from reading the code plus a green run of the plain (126 tests, 1.8s) and config (201 tests, 3.4s) tiers. **Apache and Databricks were not run** (Apache needs more memory than the dev machine has; Databricks needs a workspace), so every claim about them is from reading only.

## Global Constraints

- One pytest process per tier; never mix tiers.
- Do not run the Apache tier on the low-memory machine.
- Parametrizing reduces lines, not test count. Only deleting a case reduces count.
- Delete a test only if it adds no unique failure signal (Task 1 evidence, or the mutation check). Keep control tests that assert a guard does *not* fire. Keep `test_api_stability.py` and `test_base_job_facade_surface.py`.
- Current totals: plain 126, config 201 (327 runnable locally). Unit tiers run in ~5s combined, so the payoff is maintenance, not speed. Runtime cost lives in Apache and Databricks.
- Static estimate, **not measured**: roughly 20-30 removable tests in plain+config. Replace with the Task 1 number.

---

### Task 1: Measure unique coverage per test (plain + config)

- [ ] Run each tier separately with per-test coverage contexts, output outside the repo:
  `COVERAGE_FILE=<scratch>/cov_<tier> uv run --with pytest-cov pytest tests/unit/<tier> -q -p no:cacheprovider --cov=fabricks --cov-branch --cov-context=test --cov-report=`
- [ ] For each test, compute lines/branches covered by no other test. Zero unique coverage = deletion candidate.
- [ ] For each candidate, confirm with the mutation check: delete it, break the guarded code, see whether anything goes red.
- [ ] Record the real removable count here, replacing the estimate above.

### Task 2: Quick cleanups (minutes)

- [ ] Delete `tests/boundary_fakes/` (only `__pycache__`, nothing tracked).
- [ ] Trim the ~60-line docstring in `tests/unit/config/conftest.py` to the four seams and the import-order rule; drop the plan-file reference and the "Confirmed empirically" narrative.
- [ ] Check `tests/unit/plain/runtests.py` (Databricks notebook): it lists only `test_git_path` and `test_variable_substitution` of 15 plain files. Decide: stale, or a silent coverage gap on Databricks.

### Task 3: Remove redundant tests

Candidates, each subject to Task 1 evidence:

- [ ] `test_cdc_context.py` (47 tests): keep one case per distinct branch plus boundaries in the `soft_delete`, `metadata`, `correct_valid_from`, `order_duplicate_by` groups. Target ~30.
- [ ] `test_invoker_transient_retry.py` (6) and `test_invoker_warn_on_error.py` (position x exception cross-product): one test per behavior.
- [ ] Mock-shape tests with a real-Spark counterpart: `test_dag_dependency_status` (substring on mocked SQL), `test_merge_query_caches_target_view`. Keep only where no Apache counterpart exists.
- [ ] `test_incremental_filter_full_scan.py`: self-described characterization test of current behavior; delete unless something depends on it.
- [ ] Review `test_fixture_locality.py` (AST walker, ~80 lines guarding a convention) with `ponytail-review`; a grep-style check may suffice.

### Task 4: Merge files by topic (lines/files, not count)

- [ ] DAG + retry cluster (9 files) into `test_dags.py` and `test_invoker_retry.py`, sharing `_dag_processor_helpers.py`.
- [ ] Skip/unchanged cluster (`test_job_run_unchanged`, `test_post_run_unchanged`, `test_dag_receive_skips_unchanged`, `test_silver_skip_if_stale_option`) into `test_skip_unchanged.py`.
- [ ] Tiny files into their topic: `test_gold_table_option` into `test_option_hierarchy`; `test_stream_processor`, `test_get_job_orphan`, `test_no_drop`, `test_timeout`, `test_get_job_conf_cache` into `test_job_facade.py`; Apache `test_config_loads` and `test_get_step` into `test_get_job.py`.
- [ ] Group query-shape regressions as `test_query_plans.py` in config and Apache so each mock check sits beside its real-Spark counterpart.

### Task 5: Layout and tier boundaries

- [ ] Move `validate_scenario` and `apache_fixture_paths` (used by `unit/plain/test_cdc_harness.py` and `test_generate_local_fixtures.py`) out of `spark/apache/` into a neutral `tests/support/`, so plain no longer imports Apache code.
- [ ] Decide on `tests/spark/runtime/` (used by config and apache) vs `tests/spark/databricks/runtime/`: consider `tests/runtime/local/` since a `unit` tier also consumes it.
- [ ] Document the intentional `tests/tier_policy.py` re-export of `tests/spark/databricks/tier_policy.py` (or collapse if the sibling-import constraint no longer holds).
- [ ] Drop `pytest-xdist` from dependencies or note it is Apache-only (justfile never uses `-n`).

### Task 6: Keep it from regrowing

- [ ] In review: a new bug-fix test should say which older characterization test it replaces.
- [ ] Follow `docs/TEST.md`: smallest tier that proves the behavior; no Databricks test if unit or Apache can.

---

## Verification (after each task)

- [ ] `just test-plain` and `just test-config` green (separate invocations).
- [ ] For deleted tests: mutation check recorded in the commit message.
- [ ] Apache tier: run on a machine with enough memory before merging Tasks 3-5 that touch `spark/apache/`.
