---
name: fabricks-project
description: Use for Fabricks runtime project setup and scheduling — locating/writing fabricksconfig.json or pyproject.toml's [tool.fabricks] (including job_config_from_yaml), the main conf YAML's step/timeout/worker/spark_options structure, tags-based schedule selection, options.type: manual, and options.wait_for. Not for a single job's YAML shape — see fabricks for that.
---

# Fabricks Project Setup & Scheduling

**REQUIRED BACKGROUND:** `fabricks` for job config basics.

## Runtime Config

The main conf YAML (name varies per repo, e.g. `conf.premium.yml`, not a
fixed filename) defines steps, timeouts, workers, and spark options.
Each step block sets its own `path_options.runtime` (where its YAML/SQL
live) and `path_options.storage`.

Fabricks locates that file by walking up from cwd for
`fabricksconfig.json` first, falling back to `pyproject.toml`'s
`[tool.fabricks]`. **Preferred: `fabricksconfig.json`** — use it over
`pyproject.toml` for new repos.

```json
{
  "runtime": ".",
  "notebooks": "notebooks",
  "config": "fabricks/conf.premium.yml",
  "job_config_from_yaml": false
}
```

**`job_config_from_yaml` is important.** It decides where a job's config
comes from at runtime:

- `false` (default) — read from the deployed `fabricks.<step>_jobs`
  metastore table. A YAML edit does **nothing** until that table is
  reloaded (`get_step(step).update_configurations()`, or
  `Deploy.step(step)` — see `fabricks-api`). This is what production
  should use.
- `true` — read directly from the YAML files on disk, every time a job
  is resolved, **but not actually live**: file reads are cached process-wide
  (`functools.lru_cache`, keyed by path). In a long-lived session (a
  notebook, a REPL) an edit to a file already read in that process still
  won't show up. Clear the state — restart the Python process
  (`dbutils.library.restartPython()` in a notebook) — after editing YAML,
  or you'll be debugging a change that "isn't taking effect" for the
  wrong reason. Best for local dev/testing regardless; not production.

## Tags & Scheduling

`tags: [name, ...]` on a job's main config (not `options:`) is mainly how
**schedules** select which jobs to run — a schedule declares
`options.tag: <name>`, and it runs every job whose `tags:` list contains
that name (`array_contains`), on top of whatever `steps:`/`view:`
filtering the schedule also applies:

```yaml
# job config
- job:
    step: gold
    topic: sales
    item: daily_summary
    tags: [cip_prod]
    options:
      mode: complete
```

```yaml
# any *.yml under the step conf's path_options.schedules dir, under a
# `schedule:` key -- the filename itself isn't fixed/enforced
- schedule:
    name: cip_production
    options:
      tag: cip_prod
```

Editing `schedules.yml` alone does nothing — run `Deploy.schedules()`
(see `fabricks-api`) to make the change effective.

`options.type: manual` on a job excludes it from the default
schedule/dependency views entirely (`type not in ('manual')` is baked
into the schedule-view SQL) — it has to be run explicitly, never picked
up automatically. Use it for one-off or hand-triggered jobs.

`options.wait_for: [step.topic_item, ...]` adds extra ordering
dependencies on top of `parents:`/DAG inference, without those tables
being real data dependencies — the common case is a `mode: invoke` job
that needs to run after a batch of unrelated jobs finish (e.g. a
PowerBI dataset refresh that must wait for every table it reads to be
done, not just the ones `parents:` would capture).

## Validating Config Offline

`python -m fabricks.runchecks` (from the runtime repo root, `fabricks`
installed) catches wiring mistakes without a Databricks connection: missing
`.sql`/notebook files, dangling `parents:` references, unparseable SQL,
duplicate table names, circular dependencies between jobs (`parents:` if
declared, else the tables the job's own SQL reads from — same rule
Fabricks itself uses to build the DAG), and a SQL reference to a
nonexistent table in a fabricks-managed database (its database matches a
declared step name — an error, since the full set of fabricks-managed
tables is knowable) vs. an unmanaged database (a warning only, since it
could be a legitimate external/raw source). Bronze and silver never
author SQL either way — it auto-discovers which step names those are the
same way Fabricks itself finds its config (walk up for
`fabricksconfig.json`/`pyproject.toml`, read the `bronze:`/`silver:`
lists in the conf YAML it points to); pass `--config <path>` to point at
that conf YAML directly if discovery picks the wrong one, and
`--passthrough-step <name>` (repeatable) on top for anything discovery
can't know about — it warns (doesn't fail) when neither `fabricksconfig.json`
nor `pyproject.toml` is found at all. `--verbose` adds a per-step table
(job count, topic count, `.sql` file count). Wire it into CI right after
`databricks bundle validate`.
