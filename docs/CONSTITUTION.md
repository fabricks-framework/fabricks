# Constitution

Coding and repo-governance rules for `framework/`. [AGENTS.md](../AGENTS.md) is
the entry point; this file is what it points to for "the rules."

## I. Precedence

Direct user instruction > this file > the skills listed in `AGENTS.md` >
default agent behavior. If a rule here conflicts with what a skill would
otherwise do, this file wins for anything inside `framework/`.

## II. The Databricks LTS ceiling

`framework/pyproject.toml` pins every dependency at or below the version
shipped in the current Databricks LTS runtime (see the comment above
`[project.dependencies]`) — e.g. PyYAML stays at `6.0.x` because that's what
LTS ships. **Never bump a dependency's minimum past what LTS provides**,
even to pick up a fix or a nicer API, without checking the target LTS
runtime's preinstalled package list first. A version that's fine on your
machine can break every runtime repo the moment it deploys to a cluster.

## III. Code style

Enforced by `ruff` (`ruff.toml`, `[tool.ruff]` in `pyproject.toml`) and `ty`,
run via `just lint` / `just format` — don't hand-format against a
remembered style, run the tools and treat a clean pass as the bar. Two
things worth knowing without reading the config:

- Explicit typing is expected on non-test code; `tests/*` is exempt.
- Prefer the stdlib/`pathlib` over ad-hoc string path handling, and let a
  simplify/bugbear finding change the control flow rather than silencing it.

> Note: a couple of `ruff.toml` entries are leftover from a template it was
> copied from (a stray comment, an isort `known-first-party` that doesn't
> name this package). Don't treat those specific lines as intentional
> project convention.

## IV. Respect the layer boundary

Import through `api/` from a runtime's own code (parsers, UDFs, extenders,
notebooks) — never reach into `core`/`context`/`cdc`/`metastore` directly.
Never import `core` from `metastore`; if that feels necessary, the change
belongs in `core` instead. See [ARCHITECTURE.md](./ARCHITECTURE.md)'s
package table for why each boundary exists and what a change on either side
of it actually touches.

## V. Testing discipline

See [TEST.md § Rule of thumb](./TEST.md) for the unit-vs-integration
decision and when a change needs a test at all.

## VI. Where new documentation goes

New documentation (a new integration, subsystem, or runbook topic) goes in
`docs/`, never appended directly into `AGENTS.md` — `AGENTS.md` stays a
pointer list. Pick the file by what the content *is*, not by what prompted
it:

- System structure, package layout, how a request flows through the code →
  [ARCHITECTURE.md](./ARCHITECTURE.md).
- A coding rule, a repo-governance rule, a "never do X" that should bind
  future work → this file.
- Test layout, fixtures, how to run a suite → [TEST.md](./TEST.md).
- A bug's signature/symptom and root cause, once diagnosed, so the next
  agent recognizes it faster → [DEBUG.md](./DEBUG.md). One entry per bug;
  symptom first, root cause second, fix or workaround last.

If none of the four fit — a new integration, a new subsystem with its own
concerns — add a new focused file under `docs/` and add one line for it to
the pointer list at the top of `AGENTS.md`. Don't fold unrelated content
into one of the four existing files just to avoid creating a fifth.
