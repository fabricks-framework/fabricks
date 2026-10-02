# Dev Workflow

Agent-facing runbooks for recurring tasks. See [TEST.md](./TEST.md) for tier
details and [CONSTITUTION.md](./CONSTITUTION.md) for the rules these
workflows must stay inside.

## Code Navigation

1. When a `.codegraph/` index is available and the task needs code discovery,
   call-path tracing, dependency analysis, or cross-file impact assessment,
   use CodeGraph first.
2. When available, use Serena for precise symbol inspection, diagnostics, and
   edits.
3. When available, use Headroom to compress large retrieved context; it does
   not replace code navigation.
4. If CodeGraph or Serena is unavailable, use the standard file-search tools.

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
5. Run `/simplify` against the diff and apply what it finds.

## Models and effort

Use Haiku and Sonnet only. Complexity is handled by raising effort, never by switching to a bigger model.

| Action | Model | Effort |
|---|---|---|
| Search the repo, read many files, summarise logs or CI output, run a command and report | Haiku | low |
| Mechanical edits with a clear spec (lint hits, `noqa`, comment shortening, renames, docs numbers) | Haiku, or Sonnet if it spans several files | low |
| Normal implementation and bugfixes, tests inside an existing tier | Sonnet | medium |
| `fabricks/cdc/`, CDC guard tests, SQL templates, Spark plans | Sonnet | high |
| Design trade-offs, reviewing a diff against the spec, multi-file refactors | Sonnet | high, xhigh for architecture-level calls |
| Mutation gates and test pruning | Sonnet decides at high; Haiku at low runs and collects results | |

Escalation:

1. Start at the lowest effort whose output you can check mechanically (`just lint`, the unit tiers, a test that
   fails first and then passes).
2. If the check fails twice, raise Sonnet's effort one level (low, medium, high, xhigh, max) instead of retrying
   at the same level.
3. Haiku stays at low. If its result fails once, redo the step on Sonnet.

Subagents:

- Spawn one for fan-out search or reading across several files, for log and CI triage, and for independent
  tasks that share no state. Do not spawn one for a single-fact lookup you can search directly.
- `explorer` (Haiku, `.claude/agents/explorer.md`): read-only search and log triage. It returns a conclusion
  with `file:line` references, not file dumps.
- `cdc-reviewer` (Sonnet, `.claude/agents/cdc-reviewer.md`): reviews a diff under `fabricks/cdc/` or the CDC guard
  tests against `testing-fabricks` and `docs/CONSTITUTION.md`. It proposes changes and never edits.
- `ci-triage` (Haiku): reads a GitHub Actions run and reports failures, timings and stuck runs.
- `mutation-gate` (Sonnet, own worktree): applies mutants and compares which old and new tests go red.
- `test-pruner` (Sonnet): ranks merge or delete candidates from timing and coverage-overlap data; proposes only.
- Launch independent subagents in one message so they run in parallel, and give each a self-contained prompt.
- Subagent definitions set `model` but not effort, so effort follows the session; put the depth you need in the prompt.
