# Testing

Four tiers under `framework/tests/`, split by whether a test needs
`fabricks.context`/Spark at all, and if so, how real that Spark needs to
be. Each tier is plain `pytest`, distinguished by marker (`plain` /
`config` / `apache` / `databricks`, declared in `pyproject.toml`
`[tool.pytest.ini_options]`); each tier's `conftest.py` auto-applies its
own marker to every test under it, so you don't need to tag tests by hand.

**Do not mix tiers in one `pytest` invocation.** Each tier's `conftest.py`
mutates `sys.modules`/patches Spark at *module import time*, before any
test file in that directory is collected — whichever tier's conftest runs
first in a process wins for the rest of that process. Run each tier as its
own separate `pytest` command.

## Plain tests — `tests/plain/`

Pure Python, no `fabricks.context`/Spark dependency at all.
`tests/plain/conftest.py` mocks `fabricks.utils.spark` / `fabricks.context`
wholesale at module level before any `fabricks` import happens, so code
under test never touches a real `SparkSession` — and never sees real
`STEPS`/`CONF_RUNTIME` either. Use these for logic that doesn't need Spark
or real runtime config to prove itself: path handling (`test_git_path.py`),
YAML/config parsing (`test_read_yaml.py`), variable substitution
(`test_variable_substitution.py`), pure `sqlglot` SQL parsing
(`test_sql_dependencies.py`).

```
uv run pytest tests/plain
```

## Config-resolution tests — `tests/spark/config/`

Real `fabricks.context`/`get_step()`/`get_job()` against real YAML config,
with Spark faked out so no JVM (and therefore no Java installation) is
needed. `tests/spark/config/conftest.py` patches two independent
Spark-construction paths — `pyspark.sql.SparkSession.builder` and
`fabricks.utils.spark`'s own eager `get_spark()` call — see that file's
docstring for why both are necessary. Use these for job/step *decision
logic* that reads real config but never needs to execute against real
data: DDL/`table_options` mapping (`test_ddl_option_mapping.py`), the
job-vs-step option-precedence mechanism (`test_option_hierarchy.py`),
partition/cluster column auto-detection (`test_column_selection.py`),
check comparison logic and messages (`test_checker.py`).

```
uv run pytest tests/spark/config
```

## Apache Spark tests — `tests/spark/apache/`

Real Spark+Delta (Apache Spark — stock open source, as opposed to
Databricks Runtime), running in a podman container (needs a JVM, which
this repo's default dev venv doesn't provide). `tests/spark/apache/conftest.py`
builds a real Delta-configured `SparkSession` at module-import time against
a fixture runtime checked into this repo at
`tests/spark/apache/runtime/fabricks/conf.fabricks.yml`. Use these for CDC
merge correctness and DDL that genuinely needs real Delta table state to
verify (real row counts, real table features) — not just the DDL/config
*generation* logic, which belongs in `tests/spark/config/` instead.

```
podman compose -f framework/tests/spark/apache/docker-compose.yml run --rm apache-tests
```

## Databricks integration tests — `tests/spark/databricks/`

Exercise real jobs end-to-end against a real Databricks cluster
(`databricks-connect`) and a real fixture runtime checked into this repo at
`tests/spark/databricks/runtime/` (its own `bronze/`, `silver/`, `gold/`,
parsers, UDFs, extenders, masks, views, schedules, and
`conf.uc.fabricks.yml` / `conf.5589296195699698.yml`). They are not
self-contained pytest — they run as Databricks notebooks
(`# Databricks notebook source` header, driven by `dbutils.widgets`),
because they need a live cluster with the runtime deployed:

- `tests/spark/databricks/runtests.py` — the entry notebook. Widgets
  control `initialize` / `armageddon` (full data reset) / `reset` /
  `fix_notebooks`, and which of `job1`..`job5` to run. It resolves to
  `pytest -k <selection>` under `jobs/`.
- `tests/spark/databricks/init.sh` — cluster init script; sets
  `FABRICKS_RUNTIME` / `FABRICKS_NOTEBOOKS` / `FABRICKS_CONFIG` env vars and
  pip-installs the test/runtime dependencies onto the cluster.
- `tests/spark/databricks/jobs/job1/…job5/` — the actual test modules,
  ordered with `pytest.mark.order(...)` (via `pytest-order`) because later
  jobs depend on tables earlier jobs produced (schedules, CDC reload,
  invoke, dependency resolution, checks, etc. each get their own
  `test_*.py`).
- `tests/spark/expected/{silver,gold}/{scd0,scd1,scd2}/iter*.sql` — golden
  SQL snapshots, a sibling of `tests/spark/databricks/`/`tests/spark/apache/`
  under `tests/spark/` (shared, not owned by either suite). `compare.py`
  builds the job's generated SQL and diffs it against these; a deliberate
  SQL-generation change means regenerating the matching snapshot, not
  hand-editing it to make the diff pass.
- `tests/spark/databricks/phases/0_armageddon/` .. `phases/5_extra/` — the
  same jobs grouped into ordered phases (full reset → first schedule →
  second schedule → a plain run → CDC reload → step-level extras)
  mirroring what a real deployment does over its lifetime.

There is no local way to run these — they need the fixture runtime
deployed to an actual Databricks workspace/cluster with `init.sh` applied,
real notebook execution, or real multi-job wall-clock ordering. Keep this
tier limited to exactly that: real notebooks (`invoke_*`, `*_notebook`),
real schedule timing (`wait_for`, forced-failure/skip assertions), and real
Unity Catalog integration. If a test here is actually decision logic or
DDL generation with no real notebook/timing/UC dependency, it likely
belongs in `tests/spark/config/` or `tests/spark/apache/` instead — see
[docs/superpowers/plans/2026-09-04-test-inventory.md](superpowers/plans/2026-09-04-test-inventory.md)
for the full per-test breakdown of what's already been moved and what's
still a candidate.

## Rule of thumb

Changed SQL generation → check whether an `expected/**/iter*.sql` snapshot
needs regenerating. Otherwise, pick a tier by two questions: does it need
`fabricks.context`/real config at all (no → `plain`), and if so, does it
need to actually execute against real Delta data (no → `config`, yes →
`apache`, unless it specifically needs a real cluster/notebook/UC feature →
`databricks`). Expect to confirm a `databricks`-tier change on a cluster,
not just by reading the diff.

See [CONSTITUTION.md § V](./CONSTITUTION.md) for when a change requires a
test at all, and [ARCHITECTURE.md](./ARCHITECTURE.md) for how the pieces
under test fit together.
