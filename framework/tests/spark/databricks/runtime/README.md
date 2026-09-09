# `runtime` — the Databricks runtime

Minimal runtime for the consolidated `test_schedule.py`/`test_notebook.py`,
per
[docs/superpowers/plans/2026-09-04-databricks-cut-list.md](../../../../docs/superpowers/plans/2026-09-04-databricks-cut-list.md).
Wired in via `tests/spark/databricks/init.sh` (`FABRICKS_RUNTIME`/
`FABRICKS_CONFIG`) and `pyproject.toml`'s `[tool.fabricks]` table for local
tooling. The old topic-zoo `runtime/` + `jobs/job1-5/` suite has been moved
out to `_archive/databricks-old/` (repo root, gitignored) for reference —
**unverified against a real cluster yet**: run `test_schedule.py`/
`test_notebook.py` there for real before trusting this over the archive.

17 jobs, exactly the minimal set the cut list calls for — no topic zoo. One
test per job/feature below (`test_schedule.py` for the tagged rows,
`test_notebook.py` for the `invoke_*` ones) — a job with no dedicated
assertion is a gap, not implicit coverage via some other job's check.

| Layer | Job | Tagged? | Proves |
|---|---|---|---|
| bronze | `king_scd1` | `test` | **register** mode — external-table registration against a seeded Delta table |
| bronze | `regent_scd1` | `test` | **register** mode — external-table registration against an already-real, standing Delta table on the storage account (not per-run seeded: Unity Catalog binds a path to one table for good) |
| bronze | `queen_scd1` | `test` | **register** mode — external-table registration against a seeded Delta table |
| bronze | `feature_parser` | *(none)* | **parser** mode — real file parsing via the `dummy` custom parser plugin (`fabricks/parsers/dummy.py`); register mode never calls `get_parser()`, so this untagged job is the only one that proves plugin loading |
| silver | `king_scd1` | `test` | cross-layer dependency (`parents: [bronze.king_scd1]`) |
| gold | `dim_time` | `test` | dependency *target* — memory-mode gold table |
| gold | `fact_dependency` | `test` | gold-depends-on-gold, auto-detected via SQL parsing (references `gold.dim_time` + `silver.king_scd1__current`) |
| gold | `check_fail` | `test` | deliberate pre-run failure (`check_options.pre_run: true`, `__action: fail`) |
| gold | `check_skip` | `test` | forced skip (`check_options.skip: true`, `__skip: true`) |
| gold | `check_warning` | `test` | pre-run warning (`check_options.pre_run: true`, `__action: warning`) — `for_each_run()` still executes first, so the table is populated despite the warning; `fabricks/core/dags/run.py` logs this as `warned`, not `failed` |
| transf | `fact_memory` | `test` | dependency target for the manual override below |
| transf | `fact_wait_for` | `test` | manual `wait_for: [transf.fact_memory, silver.king_scd1]` override (vs. `fact_dependency`'s auto-detected pair) |
| gold | `feature_mask` | *(none)* | **column masking** — Databricks/Unity-Catalog-only, no OSS Delta equivalent (see cut-list doc's Risks #1); can never move to Apache, so it stays covered here |
| gold | `feature_cluster_by` | *(none)* | **liquid clustering** — Databricks Runtime execution-engine feature; OSS Delta writes the table-feature flag but doesn't implement real clustering, so this stays here too |
| gold | `feature_extender` | *(none)* | **extender plugin** — job-level `extender_options` applies the `dummy` extender (`fabricks/extenders/dummy.py`) via `Invoker.extend_job()` |
| gold | `feature_udf` | *(none)* | **udf plugin** — `udf_dummy` (`fabricks/udfs/dummy.sql`) registered by `deploy_udfs()`/`register_all_udfs()` and called directly in the job's SQL |
| gold | `invoke_notebook` | *(none)* | `test_notebook.py`'s success case — `mode: invoke`, one `invoker_options.run` notebook |
| gold | `invoke_failed_pre_run` | *(none)* | `test_notebook.py`'s failure case — `mode: memory`, `invoker_options.pre_run` notebook deliberately raises |
| gold | `invoke_post_run` | *(none)* | `test_notebook.py`'s post-run case — `mode: memory`, `invoker_options.post_run` notebook |

Whichever table feature genuinely *is* portable (identity columns, primary/
foreign keys, column mapping mode, column comments, zstd compression — see
cut-list doc's Risks #1) lives in `tests/spark/apache/test_feature.py`
instead, one function per feature, same split as this file's own
`feature_mask`/`feature_cluster_by`.

`feature_mask`/`feature_cluster_by`/the three `invoke_*` jobs are
deliberately untagged: none test orchestration, so they're excluded from
`schedule(tag="test")`'s job selection and invoked directly
(`get_job(...).run()`) in their own small assertion instead of running as
part of the scheduled DAG — same pattern the original
`test_gold_fact_masker_and_commenter`/`test_invoke.py` used. `feature_*`
live under their own `feature` topic rather than being folded into `fact`,
and mask/clustering are two separate jobs (not one combined job) so a
failure names exactly which feature broke. Column **comments** were
originally bundled with masking in the source test but are plain ANSI SQL
(`COMMENT '...'`, no Databricks/UC dependency) — that check belongs in the
Apache port list, not here.

`check_fail`/`check_skip`/`check_warning` share the same `check` topic —
same `Checker.check_pre_run()`/`check_skip_run()` mechanism, one job per
distinct `__action`/`__skip` outcome. The mechanism itself (does the
`__action`/`__skip`-column SQL correctly raise the right exception type) is
also proven in isolation, no schedule needed, in
`tests/unit/config/test_checker.py`.

`feature_extender`/`feature_udf`/bronze's `feature_parser` share the
untagged/direct-invoke pattern above, asserted in `test_feature.py` rather
than `test_schedule.py`. There's
also one custom view, `fabricks.dummy` (`fabricks/views/dummy.sql`), deployed
by `create_or_replace_views()` at armageddon time and asserted directly in
`test_schedule.py`'s `test_custom_view` — it isn't a job, so it has no row
in the table above.

Not included (deliberately): `semantic`/`powerbi`/`uc` steps, any topic
beyond `king`/`queen`/`regent`.
