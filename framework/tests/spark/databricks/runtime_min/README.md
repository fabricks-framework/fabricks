# `runtime_min` — draft

Draft minimal runtime for the consolidated `test_schedule.py`/
`test_notebook.py`, per
[docs/superpowers/plans/2026-09-04-databricks-cut-list.md](../../../../docs/superpowers/plans/2026-09-04-databricks-cut-list.md).
Not yet wired to any test (no `FABRICKS_RUNTIME`/`FABRICKS_CONFIG` points
here) — for review before it replaces `../runtime/`.

12 jobs, exactly the minimal set the cut list calls for — no topic zoo:

| Layer | Job | Tagged? | Proves |
|---|---|---|---|
| bronze | `king_scd1` | `test` | **parser** mode — real file parsing via the `monarch` parser plugin |
| bronze | `regent_scd1` | `test` | **register** mode — external-table registration, no parsing |
| bronze | `queen_scd1` | `test` | **memory** mode — no physical object, `create()`/`for_each_run()` no-op |
| silver | `king_scd1` | `test` | cross-layer dependency (`parents: [bronze.king_scd1]`) |
| gold | `dim_time` | `test` | dependency *target* — memory-mode gold table |
| gold | `fact_dependency` | `test` | gold-depends-on-gold, auto-detected via SQL parsing (references `gold.dim_time` + `silver.king_scd1__current`) |
| gold | `check_fail` | `test` | deliberate pre-run failure (`check_options.pre_run: true`, `__action: fail`) |
| gold | `check_skip` | `test` | forced skip (`check_options.skip: true`, `__skip: true`) |
| transf | `fact_memory` | `test` | dependency target for the manual override below |
| transf | `fact_wait_for` | `test` | manual `wait_for: [transf.fact_memory, silver.king_scd1]` override (vs. `fact_dependency`'s auto-detected pair) |
| gold | `feature_mask` | *(none)* | **column masking** — Databricks/Unity-Catalog-only, no OSS Delta equivalent (see cut-list doc's Risks #1); can never move to Apache, so it stays covered here |
| gold | `feature_cluster_by` | *(none)* | **liquid clustering** — Databricks Runtime execution-engine feature; OSS Delta writes the table-feature flag but doesn't implement real clustering, so this stays here too |

The last two are deliberately untagged: they aren't testing orchestration,
so they're excluded from `schedule(tag="test")`'s job selection and invoked
directly (`get_job(...).run()`) in their own small assertion instead of
running as part of the scheduled DAG — same pattern the original
`test_gold_fact_masker_and_commenter` used. They also live under their own
`feature` topic rather than being folded into `fact`, and mask/clustering
are two separate jobs (not one combined job) so a failure names exactly
which feature broke. Column **comments** were originally bundled with
masking in the source test but are plain ANSI SQL (`COMMENT '...'`, no
Databricks/UC dependency) — that check belongs in the Apache port list, not
here.

Not included (deliberately): `semantic`/`powerbi`/`uc` steps, any topic
beyond `king`/`queen`/`regent`, notebook-mode jobs (that's
`test_notebook.py`'s own, separate, one-notebook fixture).
