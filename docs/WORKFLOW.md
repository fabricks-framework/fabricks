# Dev Workflow

Agent-facing runbooks for recurring tasks. See [TEST.md](./TEST.md) for tier
details and [CONSTITUTION.md](./CONSTITUTION.md) for the rules these
workflows must stay inside.

## Bugfix

1. Branch from `main`, named `bugfix-issue-<NR>` for GitHub issue `<NR>`.
2. Write a test that replicates the reported failure before touching any
   fix code — a `tests/unit/config/` test, a `tests/spark/apache/` test, or
   both. Everything starts with a test; do not write the fix first.
   - The test must encode the *correct* (fixed) behavior, not pin the
     current broken behavior. Run it and confirm it fails for the reason
     the issue describes.
   - If the bug touches CDC (`fabricks/cdc/`), a mocked unit test alone is
     not enough — include a `tests/spark/apache/` test that exercises the
     real Spark/Delta path, since SQL-shape and generation bugs can pass a
     mocked test while still failing against a real engine.
3. Fix the code — the smallest change that makes the root cause impossible,
   not a patch on the specific path the issue happened to report. Grep every
   caller of the code you're changing.
4. Run the full tier(s) the new test(s) belong to and confirm everything
   passes before considering the fix done.
