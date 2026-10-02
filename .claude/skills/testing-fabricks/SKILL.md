---
name: testing-fabricks
description: Write, fix or review tests in the Fabricks repo (framework/tests). Picks the smallest tier, keeps Fabricks logic real and mocks only external boundaries, and proves every regression test can fail. Use for any test work here, including bugfix regression tests and requests to improve test quality, or to prune, merge or speed up the suite.
---

# Testing Fabricks

A good test fails when Fabricks is wrong and passes when it is right. Keep Fabricks' business behavior real: mock only external boundaries, and prove every regression test can actually fail.

Read `docs/TEST.md` and `docs/CONSTITUTION.md` § 4 first. They are the source of truth. This skill tells you how to apply them, and when the repo docs and this skill disagree, the repo docs win.

## Hard rules from the repo

- **Don't touch existing tests without approval.** Never modify or delete an existing test without explicit user approval: propose the change and wait. This covers the CDC guard tests (`test_cdc.py`, `test_gold_cdc.py`, `cdc_harness.py`, `test_cdc_harness.py`, `test_cdc_query_generation.py`, `test_cdc_context.py`), which need extra caution.
- **Run one tier per pytest invocation.** Each tier sets global Spark and context state at collection time, and `tests/tier_policy.py` raises a `UsageError` if tiers are mixed.
- **Run from `framework/`:** `just test-plain`, `just test-config` (or `just test-unit` for both, as two processes), `just test-apache` or `just test-databricks`. Each takes an optional `TARGET` such as `tests/unit/config/test_x.py::test_y`. Logs go to `framework/.logs/<tier>/latest.log` (disable with `FABRICKS_TEST_LOG=off`).
- **Respect the import seam.** Never import `databricks.sdk.runtime` at module level in production code. `tests/unit/plain/test_seams.py` enforces this.
- **Plain tests and `tests/support/` never import `tests.spark.*`.** Pure helpers shared across tiers go in `tests/support/`. `tests/unit/plain/test_tier_boundary.py` enforces this.

## 1. Pick the smallest tier that proves the behavior

| Behavior under test | Tier | Location |
|---|---|---|
| Pure Python, parsing, YAML or variable handling, inspecting SQL text | plain | `tests/unit/plain/` |
| Config resolution, option hierarchy, real `get_step`/`get_job` against real YAML, SQL generation, DAG and queue logic | config (real `fabricks.context`, fake Spark) | `tests/unit/config/` |
| Delta correctness, MERGE, CDC (scd1/scd2/nocdc) results, schema evolution, real query plans | apache (real local Spark and Delta, Java 17-21) | `tests/spark/apache/` |
| Notebooks, Unity Catalog, streaming, masks, liquid clustering | databricks (deploys the bundle to the `test` workspace) | `tests/spark/databricks/` |

Rules for choosing:

- SQL generation and config decisions belong in plain or config tests. Delta correctness belongs in apache. Don't add a Databricks test when an apache or unit test can prove the same thing.
- **CDC bugs need a real engine.** A SQL-shape bug can pass a config-tier test and still fail in Spark, so any bug touching `fabricks/cdc/` needs an apache test, not only a config test.
- You can't validate the Databricks tier locally. Say so in your report instead of claiming it passes.
- **Cost counts.** A config test runs in milliseconds; an Apache case costs seconds of Spark time (`just test-apache` prints the 20 slowest). Before adding an Apache test, check that a config test plus an existing scenario can't prove it, and add cases to an existing scenario with distinct keys instead of a new test or parametrize row (as `test_cdc_multi_source.py` does with its 8-source batches).
- No local Java 17-21 or too little memory for Spark? Run the apache tier with `just test-apache-remote` (needs `FABRICKS_REMOTE` in `framework/.env`; see `docs/TEST.md`).

## 2. Workflow

For a bugfix, follow `docs/WORKFLOW.md`: branch `bugfix-issue-<NR>`, test first, fix second.

1. **Read before writing.** Read the code under test and its callers (use CodeGraph if `.codegraph/` exists), the tier's `conftest.py`, and nearby tests. Reuse the existing helpers instead of inventing new ones:
   - `tests/semblance/` (Azure and dbutils fakes)
   - `tests/unit/config/_helpers.py` (`fake_spark()`, `src()`, `_FakeDF`, `stub_table`)
   - `tests/spark/apache/cdc_harness.py`
   - `tests/spark/expected/compare.py`
   - `tests/support/fixture_data.py`
2. **List the cases in plain words:** the reported failure, the healthy control case, and the edge cases. Typical Fabricks edge cases:
   - an empty source or slice (first run, no new bronze rows)
   - a fully empty reload or truncate
   - duplicate keys, and null keys or columns
   - late-arriving or out-of-order `__timestamp`
   - a reload interleaved with upserts
   - schema drift (new or removed columns)
   - `soft_delete`, `correct_valid_from`
   - running unchanged input twice (idempotency and the skip-unchanged paths)
3. **Write the test so it encodes the correct, fixed behavior,** never the current broken output. Open the module with a docstring that links the issue and states the behavior.
4. **Run it and confirm it fails for the reason the issue describes.** A collection error or a failed import is not a reproduction.
5. **Fix with the smallest change** that makes the root cause impossible, and grep every caller of the code you change.
6. **Prove the test can fail by mutating the fix** (see section 6).
7. **Run the whole tier, or tiers,** the new tests belong to, not just the one new test.
8. **Report back:** the tier or tiers, which mutation each test catches, every mock or fake you used and why, the seconds your apache tests add (from `--durations`), anything you couldn't run (the Databricks tier, or apache without Java), and any existing test you think should change or go (proposed, not applied; see section 9).

## 3. Mocking: as little as possible

Work down this list and stop at the first option that works:

1. **Real Fabricks code with real inputs.** Use the real YAML runtime in `tests/spark/runtime/`, the real `get_job` and `get_step`, and the real SQL templates.
2. **Real local engines.** In the apache tier use the `local_spark` fixture, real Delta tables in per-worker storage, and the real CDC classes (`SCD1`, `SCD2`, `NoCDC`). Never mock Spark to test merge, CDC or SQL results.
3. **The strict `semblance` fakes** for Azure Queue and Table, `dbutils` (widgets, secrets, taskValues, `notebook.run`) and `time.sleep`. Request the `semblance` fixture. Assert on the state it records:
   - `semblance.table(...).rows(...)`
   - `semblance.queue(...).sent` and `.pending`
   - `semblance.notebook_calls`

   Don't assert that a mock was called. To run the same scenarios against a real Azurite emulator, set `FABRICKS_TEST_AZURITE_CONNECTION_STRING`.
4. **Small typed fakes** such as `_FakeDF(columns, dtypes)`, or a new dataclass fake that honors the parameters under test.
5. **`MagicMock` only as a last resort.** If you must use one, always pass `spec=`, set every attribute the code reads, and add a comment explaining why. Watch out for truthiness: an unset `MagicMock` attribute is truthy. `src()` in `_helpers.py` documents a real case: an `isEmpty()` that was never set reads as "no data" (issue #182). Always set `is_empty=` explicitly.

More rules:

- **A fake must honor the parameters being tested.** A fake queue or table that ignores filters, `after`, or pagination can't test the logic those parameters drive. If a fake doesn't model something a test needs, extend the fake strictly so that unknown input raises. Don't loosen it.
- **Restore process-wide state.** Use `monkeypatch` for `os.environ`, `sys.modules`, module attributes and caches; never assign them directly. Anything a conftest sets at import time needs a finalizer. Reset module-level caches both before and after the test. Remember that `reset_mock()` keeps `return_value` and `side_effect` unless you pass `return_value=True, side_effect=True`.
- **Pin everything the code reads.** That means the `FABRICKS_*` environment variables, the working directory (`monkeypatch.chdir(tmp_path)` when a path should be threaded through), and the clock. Import-time configuration (`fabricks.context`) breaks collection, not the test, so it belongs in the tier's conftest.
- **If something can only be tested with heavy mocking, the design is the problem.** Propose a seam to the user and wait for approval before building it: lift pure logic out of a Spark-touching function, pass the dependency in, or import lazily.

## 4. Writing the test

- **One behavior per test,** named after it, e.g. `test_truncate_closes_all_current_rows`. Keep Arrange / Act / Assert visible, and explain the expected outcome in the assertion message.
- **CDC in the apache tier:**
  - For iteration scenarios, use `run_cdc_scenario(spark, seed_from, iters, cdc)` with the king/queen fixtures (`tests/spark/fixtures/iter1..11`) and compare against the expected oracle (`expected.scd1_iter{N}` / `expected.scd2_iter{N}`, unpadded N) with `assert_dfs_equal`.
  - For focused cases, build a small DataFrame inline with `__operation` and `__timestamp` columns, call `scd.update(df, keys=..., add_key=True, ...)`, then read `scd.table.dataframe` and assert on exact rows: `__is_current`, `__valid_from`, `__valid_to`, the row count and the values.
  - Give every test its own table suffix so tests don't share Delta tables.
- **SQL generation in the config tier:** assert on the generated SQL. Normalize whitespace with `" ".join(sql.split())`, then assert on the specific CTE or predicate that must be present or absent, for example `"__sliced" not in sql`. Back any SQL-shape assertion for CDC with an apache test that executes it.
- **Use exact assertions.** Use `==` on lists of rows or values, `len(rows) == n` together with the values, and `pytest.raises(X, match=...)`. Never use bare `assert rows`, `is not None`, or `isinstance` alone.
- **Sort or key Spark results before comparing.** Row order isn't guaranteed.
- Use `@pytest.mark.parametrize` with readable `id=` labels for table-driven cases, such as the cdc modes (`nocdc`, `scd1`, `scd2`) or option combinations.

## 5. Tests that lie: avoid false greens

- **Conditional assertions.** `if table.exists(): assert ...` passes when setup silently broke. Assert the precondition first.
- **Over-permissive checks.** An `or` that can't fail, or a substring that also matches the wrong output (`"id" in sql` matches `__valid_id`). Anchor on the exact clause.
- **Presence checks are blind to duplicates.** `assert X in sql` passes even if X appears twice and conflicts. Extract every occurrence and compare the whole list, for example with `re.findall` on MERGE clauses or columns.
- **Mocking the thing under test.** Patching a Fabricks internal and asserting on its arguments locks in an implementation detail. Mock at the boundary and assert on the result.
- **Unseeded fakes or `MagicMock`s silently answering "no data"** or "truthy" (see #182).
- **Tests written around a bug.** Wrapping a call in `try/except` documents the bug as acceptable. Assert the correct behavior. To track a known bug, use `xfail(strict=True)` with the issue number.
- **No-op gates.** A wrong `TARGET`, deselecting markers, or `skip` "because flaky" can hide real failures. Check the pytest summary line for the number of tests that actually ran.
- **Empty means error, not all clear.** If a filter (steps, jobs, checks) leaves nothing to run, the code must not report success. Test that path with a known-bad input.

## 6. Prove each test can fail

- **Mutate the fix.** Revert the fix (with `git stash`, or by hand), run only the new test, confirm it goes red for the right reason, then restore the fix.
- **Mutate each half of a compound guard separately,** e.g. the slice reset and the `has_rows` check. If reverting one half leaves the suite green, that is a test gap.
- **Mutate guards in both directions:** "never fires" (the original bug) and "always fires" (too broad). Keep the healthy control test, for example "a non-empty slice still generates the `__sliced` CTE" or "a healthy run logs no warning". It looks pointless, but it is the only test that catches the too-broad mutation. Name the pairing in the PR.
- **Check presence with `is None`, not truthiness.** `[]`, `0` and `""` are valid healthy values, and checking truthiness is how a guard ends up firing too often.
- **Know what red looks like.** A removed retry cap hangs instead of failing, so run with `timeout`. An error at collection time means no test ran at all.
- Line coverage does not replace this: in this suite most option-mapping tests have zero uniquely covered lines yet each asserts a different outcome.

## 7. Choose data that tells the bug from the fix

Before asserting, work out what the buggy code would produce from your data. If it gives the same answer as the fixed code, change the data.

- **For SCD2 ordering or `correct_valid_from`,** use at least three versions of a key with strictly increasing, distinct `__timestamp` values and distinct attribute values. A single version can't tell first from last.
- **For per-key logic** (rectify, dedup, latest), interleave at least two keys whose per-key and global orderings disagree.
- **Don't build test data that reproduces the bug's magic value.** For example, if a missing value falls back to the same default the bug produces, the test can't tell them apart.
- **Prefer real inputs.** The checked-in king/queen fixtures and the expected oracle are real data. A hand-written fixture shares the author's assumptions.
- **Record the sequence, not just the fact.** Collect `time.sleep` arguments, queue sends or notebook calls in order, and assert the whole list.
- **Measure, never estimate.** If you pin a number, compute it with real Spark and paste the result. Prefer assertions on relationships, such as the row count staying the same across a rerun.

## 8. Changing behavior an existing test already pins

Before you change any output, grep `tests/` for a distinctive slice of the old output, not just the test the issue mentions. Then classify each assertion that matches:

1. **Shape-only** (`isinstance`, `>= 0`): it was never real coverage.
2. **Pins the correct value:** your change is the regression. Stop.
3. **Pins the buggy value:** the test needs to change. Propose the change and wait for approval, per CONSTITUTION § 4.

Only the issue, the docstring, `docs/decisions/` or `docs/DEBUG.md` can tell you whether case 2 or case 3 applies. If the intent isn't written down anywhere, writing it down is part of the fix.

## 9. Pruning and consolidating

The suite only shrinks if someone removes tests, and CI time follows the apache tier. Look there first, and propose every change: the hard rule on existing tests still applies.

- **Removal classes:** *duplicate* (another test goes red under the same mutation), *weak* and *brittle* (section 5), *trivial* (passes against a broken implementation), *orphan* (guards code that no longer exists), *improbable* (input an upstream boundary already rejects).
- **Delete gate, all must hold:** reverting each fix the test guards still turns another named test red; one sentence says why it is redundant; the user approved. Keep the "which mutation each test catches" line from step 8 in the PR, as it is the evidence a later prune needs.
- **Line coverage is only a candidate list, never evidence** (section 6). `just test-overlap [TARGET]` (one tier per run) lists tests with identical line sets, for example the per-iteration `test_scd1_update` and `test_scd2_update` cases. Parametrized rows with different data land in the same group too, so check the asserted outcomes before calling a group redundant.
- **Prefer merging to deleting:** chain cases into one scenario with distinct keys, or move the check to a cheaper tier.
- **mutmut** is only practical on pure-Python and SQL-template code in the plain and config tiers; with Spark it is too slow.
- **Ask of every test:** what production bug would this catch? No answer makes it a candidate.

## Checklist

- [ ] Smallest tier chosen; any bug touching `fabricks/cdc/` has an apache test.
- [ ] Apache cost justified: seconds added are stated, and cases are batched into an existing scenario where possible.
- [ ] Each new test names the production bug it catches; pruning candidates were proposed with the section 9 gate, not applied.
- [ ] Test written first, encodes the fixed behavior, and was seen failing for the issue's reason.
- [ ] No new mocks of Spark or of Fabricks internals; `semblance` or typed fakes used for boundaries; every unavoidable `MagicMock` has `spec=` and a reason.
- [ ] Exact assertions on rows and values; Spark results sorted or keyed; no conditional asserts.
- [ ] Fix mutated: each part caught by a named test, guards mutated in both directions, healthy control test kept.
- [ ] Test data separates the buggy result from the fixed one.
- [ ] Process state restored via `monkeypatch` or finalizers; nothing read from the environment without pinning it.
- [ ] Whole tier run with `just test-<tier>`; anything not run is stated.
- [ ] No existing test modified or deleted without approval.
