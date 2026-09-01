# AGENTS.md

Agent-facing runbook for Fabricks — a pointer list, not a growing wiki. New
documentation (a new integration, subsystem, or runbook topic) belongs in
`docs/`, not appended here directly — see
[docs/CONSTITUTION.md § VI](./docs/CONSTITUTION.md) for the rule and how to
pick which `docs/` file it belongs in.

- for system structure see [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md);
- for coding rules see [docs/CONSTITUTION.md](./docs/CONSTITUTION.md);
- for testing see [docs/TEST.md](./docs/TEST.md);

- for hard-won bug signatures see [docs/DEBUG.md](./docs/DEBUG.md);
- for design decisions on subsystems not yet (or only partly) built see
  [docs/adr/](./docs/adr/);


## Inline documentation

Keep comments and docstrings to the bare minimum — one line only when it
explains a non-obvious *why* (a workaround, an invariant, a bug that would
otherwise come back). Never restate what the code already says.

## Skills

Checked into `.claude/skills/`. 
Each entry below gives **when** it fires:

- `using-superpowers` (from the `superpowers-marketplace`) — at the start of
  any task. Entry point for the superpowers skill set (brainstorming,
  systematic-debugging, tdd, writing-plans, ...); check it before acting to
  see whether one of those skills applies.
- `ponytail` — whenever writing or reviewing implementation code, or the
  user says "ponytail"/"be lazy"/"yagni". Entry point for the ponytail
  skill set: lazy-first coding (stdlib/native before custom code, shortest
  working diff), plus `ponytail-review`/`ponytail-audit` (over-engineering
  review) and `ponytail-debt` (deferred-shortcut ledger). Default posture
  for implementation work.
- `code-review` — when reviewing changes since a fixed point (commit,
  branch, tag, or merge-base). Runs Standards + Spec review side by side.
- `improve-codebase-architecture` — user-invoked only, via
  `/improve-codebase-architecture`. Scans for deepening opportunities.
- `grill-me` — user-invoked only, via `/grill-me`. Interviews a plan or
  design before it gets built.
- `api-design` — when adding or changing a service's public methods. Python
  API shape, evolution, and deprecation patterns.
- `documentation` — when writing or reviewing library docs. Docstrings,
  Sphinx, tutorials.
- `testing-strategy` — when writing or reviewing tests. Pytest suites
  (fixtures, parametrization, mocking, Hypothesis property-based testing,
  CI).
- `security-audit` — when reviewing security-sensitive code or wiring up
  scanning in CI. Bandit/pip-audit/Semgrep/detect-secrets vulnerability
  patterns (injection, hardcoded secrets, weak crypto, SSRF, XXE) and the
  bundled `scripts/security_scan.py` CI gate.
- `research` — when a topic needs investigating against primary sources
  (docs, source code, specs) rather than answered from memory. Delegates
  to a background agent, writes findings to a cited Markdown file (e.g.
  `docs/papers/`).

Built-in (bundled with Claude Code, not checked into this repo):

- `simplify` — after a diff is written. Reviews the changed code for
  reuse, simplification, efficiency, and altitude, then applies the fixes.
  Quality only, no bug-hunting — pair with `code-review` for that.

---

<!-- CODEGRAPH_START -->
## CodeGraph

In repositories indexed by CodeGraph (a `.codegraph/` directory exists at the repo root), reach for it BEFORE grep/find or reading files when you need to understand or locate code:

- **MCP tool** (when available): `codegraph_explore` answers most code questions in one call — the relevant symbols' verbatim source plus the call paths between them, including dynamic-dispatch hops grep can't follow. Name a file or symbol in the query to read its current line-numbered source. If it's listed but deferred, load it by name via tool search.
- **Shell** (always works): `codegraph explore "<symbol names or question>"` prints the same output.

If there is no `.codegraph/` directory, skip CodeGraph entirely — indexing is the user's decision.
<!-- CODEGRAPH_END -->
