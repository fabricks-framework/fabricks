# Testing

Four tiers under `framework/tests/`, grouped by two questions: does a test
need `fabricks.context` at all (**unit** vs. **integration**), and if so,
how real does Spark need to be. Each tier is plain `pytest`, distinguished
by marker (`plain` / `config` / `apache` / `databricks`, declared in
`pyproject.toml` `[tool.pytest.ini_options]`); `tests/tier_policy.py`
assigns markers from each test's path, so you don't need to tag tests by hand.

**Do not mix tiers in one `pytest` invocation.** Each tier's `conftest.py`
mutates `sys.modules`/patches Spark at *module import time*, before any
test file in that directory is collected — whichever tier's conftest runs
first in a process wins for the rest of that process. Run each tier as its
own separate `pytest` command; the shared tier policy rejects mixed runs.

## Unit tests — `tests/unit/`

Never execute against real Delta data — pure decision logic and SQL/DDL
*generation*, checked without a real table. Split into two tiers by
whether `fabricks.context` itself needs to be real.

### Plain — `tests/unit/plain/`

Pure Python, no `fabricks.context`/Spark dependency at all.
`tests/unit/plain/conftest.py` mocks `fabricks.utils.spark` /
`fabricks.context` wholesale at module level before any `fabricks` import
happens, so code under test never touches a real `SparkSession` — and
never sees real `STEPS`/`CONF_RUNTIME` either. Use these for logic that
doesn't need Spark or real runtime config to prove itself: path handling
(`test_git_path.py`), YAML/config parsing (`test_read_yaml.py`), variable
substitution (`test_variable_substitution.py`), pure `sqlglot` SQL parsing
(`test_sql_dependencies.py`).

```
uv run pytest tests/unit/plain
```

### Config-resolution — `tests/unit/config/`

Real `fabricks.context`/`get_step()`/`get_job()` against real YAML config,
with Spark faked out so no JVM (and therefore no Java installation) is
needed. `tests/unit/config/conftest.py` patches two independent
Spark-construction paths — `pyspark.sql.SparkSession.builder` and
`fabricks.utils.spark`'s own eager `get_spark()` call — see that file's
docstring for why both are necessary. Use these for job/step *decision
logic* that reads real config but never needs to execute against real
data: DDL/`table_options` mapping (`test_ddl_option_mapping.py`), the
job-vs-step option-precedence mechanism (`test_option_hierarchy.py`),
partition/cluster column auto-detection (`test_column_selection.py`),
check comparison logic and messages (`test_checker.py`), CDC-context/
generated-SQL decision logic (`test_cdc_context.py`,
`test_cdc_query_generation.py`).

Not `plain`, even though no real Spark session runs here either: this tier
still needs real `fabricks.context`/`STEPS`/`CONF_RUNTIME` (which `plain`'s
conftest mocks away wholesale, so `get_job()` there never sees real
config) and real `pyspark` types for realistic mocking, since code under
test does genuine `isinstance(x, DataFrameLike)` checks that a duck-typed
stand-in can't satisfy.

```
uv run pytest tests/unit/config
```

## Integration tests — `tests/spark/`

Execute against a real Spark session and real Delta table state, at
increasing fidelity. (Still under `tests/spark/` rather than a matching
`tests/integration/` — only the `unit/` side has been regrouped so far.)

### Apache Spark — `tests/spark/apache/`

Real Spark+Delta (Apache Spark — stock open source, as opposed to
Databricks Runtime), running natively on a local JVM — Java 17 or later.
`tests/spark/apache/conftest.py`
builds a real Delta-configured `SparkSession` at module-import time against
a fixture runtime checked into this repo at
`tests/spark/apache/runtime/fabricks/conf.fabricks.yml`. Use these for CDC
merge correctness and DDL that genuinely needs real Delta table state to
verify (real row counts, real table features) — not just the DDL/config
*generation* logic, which belongs in `tests/unit/config/` instead.

The tier also covers representative Silver/Gold CDC wiring, focused
truncate/reload recovery, and the `semantic` Gold-family step's physical
properties, partitioning, compression, and Power BI-compatible Delta
settings. CI runs this tier independently under Java 17.

```
just test-apache   # needs Java 17 or later on PATH
```

### Databricks — `tests/spark/databricks/`

Exercise the behavior that genuinely requires a live Databricks workspace:
notebook invocation, schedule ordering/status propagation, Unity Catalog
masks, liquid clustering, plugin loading, and type widening. The minimal
fixture runtime lives under `tests/spark/databricks/runtime/`.

- `runtests.py` seeds raw data, performs armageddon, and launches pytest.
- `test_schedule.py` runs one tagged schedule and asserts exact success,
  failure, skip, warning, dependency-order, and timeout outcomes.
- `test_notebook.py` covers direct run/pre-run/post-run notebook invocation.
- `test_feature.py` covers parser/extender/UDF loading, masks, liquid
  clustering, and physical type widening.
- `runtime/README.md` is the authoritative job inventory.
- `init.sh` configures the runtime and dependencies on the cluster.

There is no local way to run these — they need the fixture runtime
deployed to an actual Databricks workspace/cluster with `init.sh` applied,
real notebook execution, or real multi-job wall-clock ordering. Keep this
tier limited to exactly that: real notebooks (`invoke_*`, `*_notebook`),
real schedule timing (`wait_for`, forced-failure/skip assertions), and real
Unity Catalog integration. If a test here is actually decision logic or
DDL generation with no real notebook/timing/UC dependency, it likely
belongs in `tests/unit/config/` or `tests/spark/apache/` instead — see
[docs/superpowers/plans/2026-09-04-test-inventory.md](superpowers/plans/2026-09-04-test-inventory.md)
for the full per-test breakdown of what's already been moved and what's
still a candidate (written before the `unit/` regroup — read `tests/spark/config/`
there as `tests/unit/config/` and `tests/plain/` as `tests/unit/plain/`).

The live suite retains one schedule run with representative pre-run,
row-count, duplicate-key, timeout, skip, and warning outcomes. Direct feature
tests cover notebook invocation, parser/extender/UDF loading, masks, liquid
clustering, and type widening.

## Rule of thumb

Changed SQL generation → check whether an `expected/**/iter*.sql` snapshot
needs regenerating. Otherwise, pick a tier by two questions: does it need
`fabricks.context`/real config at all (no → `unit/plain`), and if so, does
it need to actually execute against real Delta data (no → `unit/config`,
yes → `apache`, unless it specifically needs a real cluster/notebook/UC
feature → `databricks`). Expect to confirm a `databricks`-tier change on a
cluster, not just by reading the diff.

See [CONSTITUTION.md § V](./CONSTITUTION.md) for when a change requires a
test at all, and [ARCHITECTURE.md](./ARCHITECTURE.md) for how the pieces
under test fit together.
