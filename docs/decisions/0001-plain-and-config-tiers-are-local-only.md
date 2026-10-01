# 0001: The plain and config tiers are local-only

## Context

`tests/unit/plain/runtests.py` was a Databricks notebook runner that listed two
of the plain test files. It looked like a coverage gap on Databricks. It was
not: `databricks.yml` excludes `tests/unit/**` from the bundle sync (plain
fixtures hold an `.ipynb`/`.py` notebook pair that sync maps to one remote path
and rejects), so the notebook was never deployed, nothing referenced it, and its
`%run ../integration/add_missing_modules` target did not exist. The plain tier
also replaces `fabricks.context` with a mock at import, so it cannot exercise a
real cluster anyway.

## Decision

Plain and config run only on a developer machine and in CI (`just test-plain`,
`just test-config`). Only the Databricks tier runs on a cluster, through
`tests/spark/databricks/runtests.py`. The dead plain runner was deleted rather
than kept in sync.

## Consequences

- A behavior that matters on a cluster needs a Databricks-tier test, or an
  Apache-tier test if real Spark and Delta are enough (see `docs/TEST.md`).
- Do not re-add a runner under `tests/unit/`; the bundle would not deploy it.
