# AGENTS.md

Agent-facing runbook for Fabricks — a pointer list, not a growing wiki. New
documentation (a new integration, subsystem, or runbook topic) belongs in
one of the tracked `docs/` files, not appended here directly — see
[docs/CONSTITUTION.md § VI](./docs/CONSTITUTION.md) for the rule and how to
pick which retained `docs/` file it belongs in.

- for system structure see [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md);
- for coding rules see [docs/CONSTITUTION.md](./docs/CONSTITUTION.md);
- for testing see [docs/TEST.md](./docs/TEST.md);
- for hard-won bug signatures see [docs/DEBUG.md](./docs/DEBUG.md);

## Skills

Checked into `.claude/skills/`, synced from the plugin cache via
`just update-skills` (run from `framework/`) per `framework/skills.json`.
Grouped by task — **when doing X, use Y**.

Loaded by *default*, every task:

- `using-superpowers` (from the `superpowers-marketplace`) — entry point for
  the superpowers skill set (brainstorming, systematic-debugging, tdd,
  writing-plans, ...); check it before acting to see whether one of those
  skills applies.
- `ponytail` — lazy-first coding (stdlib/native before custom code, shortest
  working diff); default posture for implementation work. Entry point for
  `ponytail-review`/`ponytail-audit` (over-engineering review) and
  `ponytail-debt` (deferred-shortcut ledger).

When *writing Python*:

- `coding-guidelines-python` (from `bmsuisse-skills`) — Python design/style
  beyond this repo's own formatter and lint rules.
- `code-quality` (from `python-library-complete`) — ruff/mypy config,
  type hints, and fail-loud/determinism anti-patterns.
- `api-design` — adding or changing a service's public methods. Python API
  shape, evolution, and deprecation patterns.

When *writing docs*:

- `documentation` — writing or reviewing library docs. Docstrings, Sphinx,
  tutorials.

When *testing*:

- `testing-strategy` — writing or reviewing tests. Pytest suites (fixtures,
  parametrization, mocking, Hypothesis property-based testing, CI).

When *reviewing code*:

- `code-review` — reviewing changes since a fixed point (commit, branch,
  tag, or merge-base). Runs Standards + Spec review side by side.
- `simplify` (built-in with Claude, not checked into this repo) — after a diff is
  written. Reviews the changed code for reuse, simplification, efficiency,
  and altitude, then applies the fixes. Quality only, no bug-hunting — pair
  with `code-review` for that.
- `security-audit` — reviewing security-sensitive code or wiring up
  scanning in CI. Bandit/pip-audit/Semgrep/detect-secrets vulnerability
  patterns (injection, hardcoded secrets, weak crypto, SSRF, XXE) and the
  bundled `scripts/security_scan.py` CI gate.

When *investigating*:

- `research` — a topic needs investigating against primary sources (docs,
  source code, specs) rather than answered from memory. Promote only
  durable conclusions into the retained documentation.

When *planning*:

- `improve-codebase-architecture` — user-invoked only, via
  `/improve-codebase-architecture`. Scans for deepening opportunities.
- `grill-me` — user-invoked only, via `/grill-me`. Interviews a plan or
  design before it gets built.

---

<!-- CODEGRAPH_START -->
## CodeGraph

In repositories indexed by CodeGraph (a `.codegraph/` directory exists at the repo root), reach for it BEFORE grep/find or reading files when you need to understand or locate code:

- **MCP tool** (when available): `codegraph_explore` answers most code questions in one call — the relevant symbols' verbatim source plus the call paths between them, including dynamic-dispatch hops grep can't follow. Name a file or symbol in the query to read its current line-numbered source. If it's listed but deferred, load it by name via tool search.
- **Shell** (always works): `codegraph explore "<symbol names or question>"` prints the same output.

If there is no `.codegraph/` directory, skip CodeGraph entirely — indexing is the user's decision.
<!-- CODEGRAPH_END -->
