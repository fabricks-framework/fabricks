# `runtime` — the Databricks runtime

Minimal runtime for the consolidated `test_schedule.py` and friends, per
[docs/superpowers/plans/2026-09-04-databricks-cut-list.md](../../../../docs/superpowers/plans/2026-09-04-databricks-cut-list.md).
Wired in via `pyproject.toml`'s `[tool.fabricks]` table (`FABRICKS_RUNTIME`/
`FABRICKS_CONFIG`/...) for both local tooling and the Databricks cluster --
`databricks.yml`'s job/cluster resources only set the two secrets
(`FABRICKS_ACCESS_KEY`/`FABRICKS_ENCRYPTION_KEY`) that have no config-file
fallback. The old topic-zoo `runtime/` + `jobs/job1-5/` suite has been moved
out to `_archive/databricks-old/` (repo root, gitignored) for reference --
**unverified against a real cluster yet**: run `test_schedule.py` there for
real before trusting this over the archive.

Nearly every job is tagged and runs once, together, as part of the
`tag="test"` schedule (`conftest.py`'s autouse `_schedule_run` fixture) --
not via a direct `get_job(...).run()` in an individual test -- so the
schedule can parallelize wherever the DAG allows, and every test reads
already-materialized state. `test_schedule.py` asserts the simple "did it
succeed" case for each; `test_feature.py` holds the handful of jobs that
need one deliberate extra action *after* the schedule's run (a second
invocation to prove checkpoint idempotency, a hand-crafted follow-up batch
to prove type widening) -- a job with no dedicated assertion anywhere is a
gap, not implicit coverage via some other job's check.

> [!WARNING]
> DBR 18 LTS starts each streaming `foreachBatch` callback in an isolated
> Python worker. Concurrent scheduled streams on USER_ISOLATION clusters can
> contend for sandbox startup capacity, causing `SandboxClientTimeoutTracker`
> failures and retry backoff; a test run may therefore take several minutes
> longer than its normal duration.

`feature_mask`/`feature_cluster_by` are Databricks/Unity-Catalog-only (see
cut-list doc's Risks #1) and can never move to Apache; whichever table
feature genuinely *is* portable lives in `tests/spark/apache/test_feature.py`
instead. One custom view, `fabricks.dummy` (`fabricks/views/dummy.sql`),
isn't a job -- asserted directly in `test_schedule.py`'s `test_custom_view`.

Not included (deliberately): `semantic`/`powerbi`/`uc` steps, any topic
beyond `king`/`queen`.
