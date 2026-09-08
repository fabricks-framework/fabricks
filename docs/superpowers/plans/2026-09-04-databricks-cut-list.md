# Databricks Cut List — one schedule, all CDC in Apache

A draft of §3's minimal runtime now exists at
[framework/tests/spark/databricks/runtime/](../../../framework/tests/spark/databricks/runtime/README.md)
(10 jobs, not yet wired to any test).

Companion to
[2026-09-04-test-inventory.md](2026-09-04-test-inventory.md) and
[2026-09-04-session-summary.md](2026-09-04-session-summary.md). Built from a
full re-read of every test function in `tests/spark/databricks/jobs/job1`
through `job5` (confirms the inventory doc's per-function claims; its two
file-header test *counts* were stale — `job1/test_gold.py` is 18 tests not
16, `job1/test_silver.py` is 17 not 14 — every individual row was already
correct).

Guiding call from this session: SCD0/SCD1/SCD2 merge correctness is the same
code path regardless of which topic exercises it (`monarch`, `regent`,
`memory`, `king_and_queen` all hit the identical `SCD1`/`SCD2`/`SCD0`
classes). Once that's proven once, in Apache, against real Spark+Delta,
re-proving it per topic and per `iter` on a real cluster is pure
duplication. Databricks should keep exactly **one** real schedule/
orchestration test and nothing that could instead run against local
Spark+Delta.

## 1. Delete outright (once Apache has equivalent SCD-CORE coverage)

Same merge logic, different topic/iter — no unique behavior, no port needed,
just delete once the Apache side below exists:

| File | Functions | Notes |
|---|---|---|
| `job1/test_gold.py` | `test_gold_scd1_complete`, `_update`, `test_gold_scd2_complete`, `_update`, `test_gold_scd0_update` | Blocked on: Gold SCD0/1/2 not yet ported to Apache at all (only Silver's `king_and_queen` is) — see §2 |
| `job1/test_silver.py` | `test_silver_monarch_scd2`/`scd1`, `test_silver_regent_scd2`/`scd1`, `test_silver_memory_scd2`/`scd1` | `king_and_queen_scd2`/`scd1` already ported — these are the same check, different fixture data |
| `job2/test_gold.py` | `test_gold_scd1_complete`/`_update`, `test_gold_scd2_complete`/`_update`, `test_gold_scd0_update` | iter=2 — already representable as an Apache `_SCENARIOS` entry `(1, [2], 2)` once gold is ported |
| `job2/test_silver.py` | `test_silver_monarch_scd2`/`scd1`, `regent_scd2`/`scd1`, `memory_scd2`/`scd1`, `king_and_queen_scd2`/`scd1` | `king_and_queen` iter=2 is already Apache's `(1, [2], 2)` scenario — delete now, not blocked |
| `job3/test_gold.py` | `test_gold_scd1_complete`/`_update`/`_identity`, `test_gold_scd2_complete`/`_update`, `test_gold_scd0_update` | iter=11 — blocked on §2 |
| `job3/test_silver.py` | `test_silver_regent_scd2`/`scd1`, `memory_scd2`/`scd1` | iter=11, no unique behavior. (`monarch_*` and `king_and_queen_*` in this file also carry a truncate+reload check — see §3, don't delete those two wholesale) |
| `job4/test_gold.py` | all 6 (`scd1_complete`/`_update`/`_identity`, `scd2_complete`/`_update`, `scd0_update`) | iter=11, duplicate of job3's own duplicates. Delete first regardless of §2 timing — job3 already covers the same iter |

**~35 test functions removed**, contingent on §2's gold port landing first for the `test_gold.py` rows (the `test_silver.py`/`job2` rows can be deleted immediately — Apache already covers them).

## 2. Port to Apache — real gaps (not covered anywhere yet)

These test genuinely distinct behavior. "Port" means: add as a new scenario/
assertion in the existing Apache files, generally *not* a new per-topic
file — same lesson as the `king_and_queen` redesign (seed from `expected`,
no job orchestration).

| Behavior | Source (Databricks) | Target in Apache | Priority |
|---|---|---|---|
| **Gold SCD0/1/2 merge correctness** | `job1/test_gold.py` core rows | New `tests/spark/apache/jobs/job1/test_gold.py`, same `(seed_from, jobs, compare_to)` pattern as Silver's `king_and_queen` | **First** — everything in §1's `test_gold.py` rows is blocked on this |
| NoCDC vs SCD2-shaped oracle | `job3/test_gold.py::test_gold_nocdc_update` | Extra assertion once gold port exists | Low |
| `truncate()+run()` reload idempotency | `job3/test_silver.py` (monarch, king_and_queen), `job3/test_gold.py::test_gold_scd1_memory`/`scd2_memory` | New dedicated test — needs `get_job()`/job-config layer Apache doesn't have yet (flagged in the original plan's Out of Scope) | Real gap, not just a duplicate |
| `__valid_from` sentinel correction | `job1/test_gold.py::test_gold_scd2_correct_valid_from` | Standalone assertion — logic already exercised implicitly by `king_and_queen`'s `correct_valid_from=True`, just never asserted directly | Small |
| UDF column | `test_gold_fact_udf` | New scenario/topic | — |
| Memory-mode column hygiene | `test_gold_fact_memory` | New scenario/topic | — |
| Dedup (order-column, plain) | `test_gold_fact_order_duplicate`/`deduplicate`, `test_silver_princess_order_duplicate` | New scenario | — |
| Manual job type (no-op table) | `test_gold_fact_manual` | New scenario | — |
| Identity columns (real table) | `test_gold_dim_identity`/`dim_date`, `test_gold_dim_overwrite` | New scenario — DDL half already CONFIG-done | — |
| Table options/comment/**liquid clustering**/spark-options/timeout | `test_gold_fact_option`, `test_silver_timeout` | New scenario for comment/spark-options/timeout/`cluster_by`-DDL-shape — **liquid clustering's real behavior stays Databricks-only** (see Risks: it's a Databricks Runtime execution-engine feature, not part of open-source Delta — OSS Spark can write the `clustering` table feature flag but doesn't implement real clustering) | — |
| SCD1 last-timestamp bookkeeping | `test_gold_scd1_last_timestamp` | New scenario | — |
| `no_drop` exception | `test_gold_fact_no_drop` | **CONFIG candidate**, not Apache — pure option+exception, no real data needed | — |
| Column masking (real values) | `test_gold_fact_masker_and_commenter` | **Cannot port — Databricks/Unity-Catalog-only feature, no OSS Delta equivalent at all.** Stays Databricks-only permanently — now its own untagged `feature_mask` job in `runtime` (see Risks) | — |
| Column comments | `test_gold_fact_masker_and_commenter` (was bundled with masking) | **Split out — this one IS portable.** Plain ANSI `COMMENT '...'` DDL, no Databricks/UC dependency, unlike the masking check it was bundled with in the original test. New scenario | — |
| Overwrite mode | `test_overwrite.py` (dim/fact) | New scenario | — |
| Append/latest modes | `test_silver_princess_append`/`latest` | New scenario/topic | — |
| Special-character columns | `test_silver_prince_special_char` | New scenario/topic | — |
| Delete-log handling | `test_silver_prince_deletelog` | New scenario/topic | — |
| Column widening + extend | `test_silver_monarch_delta`, `job2/test_gold.py::test_gold_type_widening_overwrite`/`merge` | New scenario | — |
| Schema drift (semantic table, silver `__current` view) | `job2/test_semantic.py::test_semantic_fact_schema_drift`, `job2/test_silver.py::test_silver_princess_schema_drift` | New scenario — silver's schema-drift path is already indirectly exercised (job2 introduces `newField`), just never asserted on the semantic/`__current` side | — |
| Calculated columns | `test_silver_princess_calculated_column` | New scenario | — |
| Commit/checkpoint bookkeeping | `test_silver_princess_check` | New scenario | — |
| Semantic table shape/options/compression/partitioning | `test_semantic.py` (5 functions) | New scenario — option-resolution halves already CONFIG-done for step/job option | — |
| Dependency graph persistence (catalog write) | `job1/test_dependency.py` (both), `job5/test_step.py::test_update_dependencies` | One consolidated Apache test — same underlying `update_dependencies()`/`fabricks.gold_dependencies` write, currently tested twice (job-level dependency test + step-level aggregation test) | Consolidate, don't port twice |
| DB object/view creation, job-configuration persistence | `job5/test_step.py::test_create_db_objects`/`test_update_configurations` | New Apache test — needs `fabricks` database created in `apache/conftest.py` first (noted in the inventory) | — |
| Checks (fail/warning/skip/duplicate_key/duplicate_identity) | `job1/test_check.py` | max/min/count_must_equal logic already CONFIG-done; the rest need `.pre_run.sql`/`.post_run.sql` file handling — LOCAL\* | — |

## 3. What's actually left on Databricks: two files

Re-examined once more: `test_gold_fact_dependency_notebook` doesn't execute
a notebook at all — `update_dependencies()` statically parses the notebook
*file* for parent references, same mechanism as the SQL-dependency variant.
No `dbutils.notebook.run` involved, so it moves to §2 (Apache port,
consolidated with the SQL-dependency test and `job5/test_step.py`'s
dependency test) — not kept here.

That leaves exactly two genuinely-can't-move concerns, each its own file
with a deliberately minimal, purpose-built job set spanning all three
layers (bronze → silver → gold) rather than the full topic zoo
(`monarch`/`regent`/`princess`/etc.) — those topics exist to exercise CDC
merge variety, which is now Apache's job, not orchestration:

| File | Covers | Minimal job set |
|---|---|---|
| **`test_schedule.py`** | Real DAG dependency ordering across layers and modes, forced-failure/skip bookkeeping, real `fabricks.last_schedule` state | **Bronze** (`bronze.py`'s three structurally distinct modes, each with its own lifecycle branch — `create()`/`for_each_run()` differ per mode, not just config): one **parser**-mode job (real file parsing via `get_parser()`), one **register**-mode job (`register_external_table()`, no parsing, truncate/restore/maintain short-circuited), one **memory**-mode job (no physical object at all — `create()`/`for_each_run()` are no-ops, needed to prove the schedule doesn't choke on a job with nothing to run). **Silver**: one job downstream of the parser-mode bronze job (real cross-layer `wait_for` ordering). **Gold**: at least two gold tables with a real inter-gold dependency — auto-detected via SQL parsing (`Gold.get_dependencies()` → sqlglot), the same pattern `gold.fact_dependency_sql → gold.dim_time` already uses, no `wait_for` needed for this pair specifically. Plus one job deliberately made to fail and one forced-skip job, to exercise `test_no_unforced_failure`/`forced_failures`/`no_unforced_skip`/`forced_skips`. A manual `wait_for` pair (`transf.fact_wait_for` pattern) covers the explicit-dependency-override path alongside the auto-detected one. Absorbs `test_schedule.py`'s existing 5 functions + `test_transf_fact_wait_for`'s real-ordering half (the static dependency-shape half of `wait_for` is a CONFIG candidate, already noted in the inventory) |
| **`test_notebook.py`** | Real `dbutils.notebook.run` invocation status propagation | One notebook, invoked directly (not through a schedule): asserts `done` on success, `failed` on a deliberate pre-run failure, `done` on post-run. Absorbs `test_invoke.py`'s existing 3 functions |

Net: 4 Databricks files (`test_schedule.py`, `test_invoke.py`,
`test_transf.py`, half of `test_dependency.py`) collapse into 2
(`test_schedule.py`, `test_notebook.py`), each built around the smallest
job set that proves its one concern — not one job set per topic.

### `job2/test_run.py`, `job3/test_run.py`, `job4/test_run.py` — moved to §1, delete outright

Re-examined per the user's call: these were never part of "the schedule" in
the sense above. `run_notebook(i=2)`/`run_notebook(i=3..11)` exist purely to
**advance real cluster state to iteration N** so job2/3/4's own
`test_gold.py`/`test_silver.py` could then assert against that iteration —
the real-cluster equivalent of the O(n²) replay Apache's `king_and_queen`
redesign already eliminated locally. `job1/test_schedule.py` runs
independently (order 102, before any of these) and needs none of this
advancement.

Once §1's `test_gold.py`/`test_silver.py` deletions land, nothing on
Databricks consumes iteration-2 or iteration-11 state anymore, so:
- `job2/test_run.py::test_run`, `job3/test_run.py::test_run` — pure
  now-orphaned state-advancement plumbing, delete.
- `job4/test_run.py::test_run` (`run_in_parallel` reload) — already asserted
  nothing about merge correctness (per the inventory), so it was never
  pulling weight as a concurrency check either; delete rather than keep as
  a smoke test nobody asked for.

## Net effect

- Databricks suite ends at **2 files**: `test_schedule.py` (DAG ordering,
  forced failure/skip, built on one minimal bronze→silver→gold job chain
  plus a fail/skip pair) and `test_notebook.py` (invocation status, one
  notebook). Everything else — ~35 duplicate SCD-core tests, 3 whole
  now-orphaned files (`job2/test_run.py`, `job3/test_run.py`,
  `job4/test_run.py`), `test_invoke.py`, `test_transf.py`, and half of
  `test_dependency.py` — deleted or absorbed into those two.
- Apache suite: gains a Gold merge-correctness file (mirroring Silver's
  `king_and_queen`), plus ~20 new scenario/assertion additions to existing
  files for the non-CDC features above — no new per-topic directories.
- `tests/spark/config/` gains a couple of small candidates already flagged
  in the inventory (`fact_no_drop`, dependency-shape, checks file-handling).

Sequencing: do the Gold port (§2's first row) before touching any
`test_gold.py` deletions in §1 — everything else in §1 can happen
independently and immediately.

## Risks

1. **Two features can't move to Apache at all, not just "not yet."**
   Checked every table feature §2 proposed porting against
   `fabricks/metastore/table.py`'s actual DDL and OSS Delta Lake's real
   capabilities:
   - **Column masking** (`test_gold_fact_masker_and_commenter`) — Unity
     Catalog/Databricks Runtime governance feature, no OSS Delta equivalent
     whatsoever. The real masked-value check is Databricks-only, permanently
     — now its own untagged `gold.feature_mask` job in `runtime`, kept
     separate from the (portable) column-comments check it used to be
     bundled with in the original test.
   - **Liquid clustering** (`test_gold_fact_option`'s `cluster_by`/
     `liquid_clustering`) — OSS Delta can write the `clustering` table
     feature flag, but the actual clustering *behavior* is a Databricks
     Runtime execution-engine feature vanilla Spark doesn't implement. Only
     the DDL shape is portable, not real clustering behavior — now its own
     untagged `gold.feature_cluster_by` job.
   - Both `feature_*` jobs are deliberately untagged (no `tags: [test]`):
     they don't test orchestration, so they're excluded from
     `schedule(tag="test")` and invoked directly instead — kept out of
     `test_schedule.py`'s DAG, verified in their own small assertion.
   - Everything else checked — **identity columns, primary/foreign keys,
     column mapping mode, zstd compression** — is genuine open-source Delta
     Lake protocol, confirmed against `table.py`'s DDL generation
     (`identity`/constraint/`columnMapping` clauses) and Delta's own
     changelog. Safe to port as planned. Deletion vectors aren't used
     anywhere in this repo (grep came back empty) — nothing to check.

2. **Apache OSS Spark ≠ Databricks Runtime for merge correctness in
   general.** The premise behind deleting ~35 duplicate SCD-core tests is
   "same code path, same result regardless of engine" — true for the CDC
   merge logic itself, but Photon and Databricks' own Delta optimizations
   can differ subtly from OSS Spark (type coercion, timezone handling,
   file-layout-dependent behavior). A bug that only manifests on real
   Databricks Runtime loses its only detector once the Databricks-side
   duplicate is gone.

3. **Sequencing risk.** Several `test_gold.py` deletions are gated on the
   Gold Apache port landing first (§2, row 1). Cutting before porting opens
   a real window with zero gold-merge-correctness coverage on either tier.

4. **The draft runtime (`runtime/`) is unverified.** Hand-matched
   against real existing job entries, but never schema-validated (no
   `jsonschema` in this venv) or run against an actual cluster. Don't delete
   the old Databricks tests until `test_schedule.py`/`test_notebook.py` are
   proven green against it for real.

5. **Loses the only genuine long-run/real-scale exercise.** Job2-5's
   sequential real-cluster replay (cut in §3) was the only place a bug from
   *many* real commits accumulating on one long-lived table could surface
   (Delta log growth, checkpoint/vacuum interaction, catalog drift over
   time). Apache's seeded scenarios run in a disposable container — even
   the two multi-batch scenarios don't replicate that.

6. **Shared-catalog isolation for the minimal schedule.** `check_fail`/
   `check_skip` write real rows into `fabricks.last_schedule`/
   `fabricks.jobs`. If this runs against a catalog shared with other
   concurrent test runs rather than a scoped/disposable one, the same kind
   of collision this session already hit and fixed locally (the local
   `.storage` wipe path bug) could recur on the cluster side, just harder to
   detect.

7. **This is a large, mostly irreversible diff.** Many files deleted,
   several new. Worth its own PR, reviewed carefully, not bundled with the
   Apache-side porting work — so a problem found late doesn't force
   reverting both at once.
