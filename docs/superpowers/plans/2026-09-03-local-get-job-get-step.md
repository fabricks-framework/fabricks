# Local get_job/get_step Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Naming update (2026-09-04):** the four tiers referenced throughout this
> plan were renamed after it was written: `tests/unit/` → `tests/plain/`,
> `tests/local/` → `tests/spark/apache/`, `tests/config/` →
> `tests/spark/config/`, `tests/databricks/` → `tests/spark/databricks/`.
> All of this plan's own path references below are kept as originally
> written (pre-rename) — see [TEST.md](../../TEST.md) for the current
> names and [2026-09-04-test-inventory.md](2026-09-04-test-inventory.md)
> for the up-to-date per-test breakdown.

**Goal:** Make `fabricks.core.get_step()`/`fabricks.core.get_job()` resolve
real job/step config, at three cost tiers — plain pytest with no Spark at
all, plain pytest with real config resolution and a faked Spark session,
and the full `tests/local/` container for actually reading real Delta
state — unblocking the migration of `test_dependency.py`/`test_step.py`
(and later `test_overwrite.py`/`test_semantic.py`) off the
Databricks-cluster suite without paying the slow container's cost for
checks that don't need it.

**Architecture:** `tests/local/` today (see
`docs/adr/0001-duckdb-backend-for-local-cdc-tests.md`, Stage 1) deliberately
bypasses `get_job()`/config resolution — it drives `NoCDC`/`SCD1`/`SCD2`
directly. `get_job_conf()`/`get_jobs_internal_df()`
(`framework/fabricks/core/jobs/get_job_conf.py:53`,
`framework/fabricks/core/jobs/get_jobs.py:43`) already have a YAML-only code
path gated by `IS_JOB_CONFIG_FROM_YAML` — the same one
`tests/databricks/init.sh` turns on for the cluster suite via
`FABRICKS_IS_JOB_CONFIG_FROM_YAML=TRUE`. Turning that same flag on locally,
plus adding `bronze`/`silver` steps + job YAML files under
`tests/local/runtime/`, is enough for `get_step(...)`/`get_job(step=...)`
to resolve — for the combined `king_and_queen` silver job and its two
`bronze.king`/`bronze.queen` parents individually — without touching a
Spark catalog table or Databricks Runtime at all: no fabricks source code
changes, only test config. The one wrinkle: importing `fabricks.context`
at all always builds a real `SparkSession` as an import-time side effect
(`framework/fabricks/context/spark_session.py:83`), and this host has no
Java installed — so a third tier (`tests/config/`, Task 4) patches
`pyspark.sql.SparkSession.builder` before that import happens, letting
`get_step`/`get_job` run for real against real YAML with no JVM and no
container.

**Tech Stack:**
- Task 1, Task 3, Task 5: pytest, PySpark local session + Delta 4.1,
  running inside the `tests/local/` podman container (already wired in
  `tests/local/conftest.py`).
- Task 2: plain `pytest` + PyYAML + `fabricks.models` — no Spark, no
  container, runs directly on the host.
- Task 4: plain `pytest` + a `pyspark.sql.SparkSession.builder` patch —
  no container, no Java required, but does need `pyspark` importable
  (already true in this project's `uv`-managed venv).

**Spec:** none — scoped directly from conversation analysis (see this
session's test-migration discussion); no separate spec doc.

## Status (as of this session)

Work happened out of strict task order — the `tests/config/` tier (Task 4)
was built and proven with two *different* test files than the plan
originally specified, before Tasks 1/2/3/5 were touched. Actual repo state:

- **Task 1 (env flag in `tests/local/`): not done.**
  `FABRICKS_IS_JOB_CONFIG_FROM_YAML` is not yet set in
  `framework/tests/local/conftest.py`.
- **Task 2 (pydantic YAML-shape check, no Spark): not done as originally
  scoped** (no `test_local_runtime_job_yaml.py` — it depends on Task 3's
  YAML, which doesn't exist yet either). **A different, independent unit
  test was added instead:** `framework/tests/unit/test_sql_dependencies.py`
  — see "Delivered this session" below.
- **Task 3 (bronze/silver `king`/`queen` steps + job YAML + seed fixture in
  `tests/local/`): not done.** `tests/local/runtime/fabricks/conf.fabricks.yml`
  has no `bronze:`/`silver:` step blocks yet (only the `databases:` block it
  started with, plus a new `gold:` block added for a different, smaller
  reason — see below).
- **Task 4 (the `tests/config/` tier itself): done, and expanded.** The
  directory, `conftest.py`, and `pyproject.toml` marker all exist and work
  — see "Delivered this session" for what's different from the plan's
  original code block, and for two extra test files added on top of it.
- **Task 5 (register-mode `.parse()` read in `tests/local/`): not done** —
  blocked on Task 3.

### Delivered this session (ahead of / instead of the plan above)

1. **`framework/tests/unit/test_sql_dependencies.py`** (3 tests, `tests/unit/`,
   no Spark at all) — `Gold._get_sql_dependencies()`
   (`framework/fabricks/core/jobs/gold.py:218`) calls `get_tables()`
   (`framework/fabricks/utils/sqlglot.py:35`), which is pure `sqlglot` — no
   `fabricks.context` import anywhere in that module. Tested directly
   against the real fixture SQL `tests/databricks/runtime/gold/gold/fact/
   dependency_sql.sql`, asserting the same dependency set
   `test_dependency.py`'s `test_gold_fact_dependency_sql` only proves via a
   full cluster schedule today. (Bronze/Silver don't do SQL-based
   dependency extraction at all — see Out of Scope.)

2. **`framework/tests/config/` built for real**, with a `conftest.py` that
   needed **two** patches, not the plan's one — see the corrected code
   block in Task 4 below. The second one (`fabricks.utils.spark` itself
   doing `spark = get_spark()` at its own import time) was discovered
   empirically, not anticipated by the original plan text.

3. **A real, unrelated environment bug found and fixed**: this venv's
   installed `delta-spark` 4.2.0 was missing most of its own files
   (`delta/__init__.py`, `delta/tables.py`, `delta/pip_utils.py` — present
   in its own `RECORD` but absent on disk; only `connect/`, `py.typed`,
   `version.py` were actually there). This blocked importing
   `fabricks.metastore.table` (and therefore `get_job`/`get_step`, which
   import it transitively) outside the container entirely, regardless of
   any Spark mocking. Fixed via `uv pip install --reinstall --no-cache
   delta-spark==4.2.0`. Without this fix, **none** of `tests/config/`'s
   tests can import successfully — worth confirming on any other machine
   that runs this tier for the first time.

4. **`framework/tests/config/test_ddl_option_mapping.py`** (10 tests, later
   revised — see below) — `Table._create()`'s
   (`framework/fabricks/metastore/table.py:215`) `table_options` → DDL
   mapping (`TBLPROPERTIES`, `CLUSTER BY`, identity column, primary key,
   foreign key, column masks/comments, the special-char → column-mapping
   default), captured off a mocked `spark.sql()` call instead of executing
   it against real Delta. **Revision**: the first version stubbed out
   `Table._get_ddl_columns()` (the column-list half, needs a DataFrame
   schema) entirely, which silently skipped `masks`/`comments` (both
   applied *inside* that method, per-column) and left `foreign_keys`
   untested. Fixed once it was pointed out those are exactly the
   `gold.fact_masker_and_commenter`/`gold.fact_foreign_keys` fixtures'
   whole point: `pyspark.sql.types.StructType`/`StructField` need no
   running `SparkSession` to construct (confirmed empirically) — so a
   small dataclass stand-in now carries a *real* schema instead of a stub,
   and `_get_ddl_columns` runs for real. One more finding along the way:
   `fix()` (the sqlglot DDL normalizer `Table._create()` calls, wrapped in
   `contextlib.suppress(Exception)`) silently fails to parse the
   `mask`/`comment` column-DDL shape and falls back to raw, un-normalized
   SQL — the masks/comments tests assert on that raw shape, not the
   uppercased one the other tests see. See "How would you test the DDL" in
   this session's transcript for the fuller reasoning on why this can't be
   a pure string-snapshot test the way CDC merge SQL is.

5. **`framework/tests/config/test_column_selection.py`** (6 tests) —
   `Generator._get_partitioning_columns`/`_get_clustering_columns`
   (`framework/fabricks/core/jobs/base/generator.py:211-265`): explicit
   `table_options.partition_by`/`cluster_by` wins unconditionally;
   otherwise auto-detected from `df.columns`/`df.dtypes` (`__partition*`
   prefix for partitioning; `__source`/`__is_current`/`__key`/`__hash`/
   `__cluster*` for clustering, skipping boolean-typed candidates). Pure
   functions of a dataframe's column names/dtypes — no Spark execution
   inside either method, so a plain dataclass stand-in for `df` (just
   `.columns`/`.dtypes`) is enough; no new job YAML needed, reuses
   `semantic.fact_step_option` and mutates its `table_options` in place via
   `job.conf.model_copy(...)` per scenario (not a `cached_property`, so
   this is safe — unlike `mode`/`change_data_capture`, see the note left in
   Out of Scope about `get_cdc_context`).

6. **`framework/tests/config/test_checker.py`** (8 tests) —
   `Checker.check_post_run_extra()`/`_check_run_time()`
   (`framework/fabricks/core/jobs/base/checker.py:69,160`): the
   min_rows/max_rows/count_must_equal comparison + exact error-message
   logic behind `gold.check_max_rows`/`min_rows`/`count_must_equal`, and
   the before/after run-time-window skip logic — both only ever touch
   Spark through `self.spark.sql(...).collect()[0][0]` (and, for
   `count_must_equal`, `self.spark.read.table(...).count()`), fully
   controllable on the fake session this tier's `conftest.py` installs.
   One thing this surfaced: `Configurator.spark` is a **lazily-built,
   per-instance** property (`framework/fabricks/core/jobs/base/
   configurator.py:98`) that calls `build_spark_session()` on first
   access — which itself issues a couple of unconditional `spark.sql("set
   ...")` calls as a side effect, before any test code runs. A "does not
   call `spark.sql`" assertion has to force that lazy init and
   `reset_mock()` first, or it fails for the wrong reason.

7. **`framework/tests/config/test_option_hierarchy.py`** (3 tests) —
   `Generator._get_option_hierarchy()` (`framework/fabricks/core/jobs/base/
   generator.py:17`): job-level option wins if set, else step-level, else
   `default`. Mirrors `tests/databricks/runtime/semantic/fact/
   _config.semantic.yml`'s `step_option`/`job_option` fixtures (see
   `test_semantic.py`'s `test_semantic_fact_step_option`/
   `test_semantic_fact_job_option`) — required adding a `semantic`
   gold-step (with step-level `table_options.properties`) to
   `framework/tests/local/runtime/fabricks/conf.fabricks.yml`'s new `gold:`
   block, plus `framework/tests/local/runtime/semantic/_config.fact.yml`
   (the `step_option`/`job_option` jobs). This is a lighter, independent
   local-YAML addition — it does **not** require Task 3's `bronze`/`silver`
   `king`/`queen` setup.

All of the above run together as `pytest tests/config/` (27 tests) or
`pytest tests/unit/` (39 tests, including the new SQL-dependency test) —
confirmed passing in isolation. Per Task 4's conftest docstring, **do not**
run `tests/unit/` and `tests/config/` in the same `pytest` invocation
(confirmed empirically to break collection — whichever tier's conftest
mocks/imports `fabricks.context` first wins for the whole process).

## Global Constraints

- No changes to `framework/fabricks/` — this is test-harness config only.
  `IS_JOB_CONFIG_FROM_YAML` and the local-runtime layout already support
  everything this plan needs.
- Follow `tests/local/`'s existing pattern: env vars set at *module import
  time* in `conftest.py`, before `fabricks.context` is first imported (see
  `tests/local/conftest.py`'s own docstring on the SPARK-singleton race —
  the same race applies to any `fabricks.context` constant, including
  `IS_JOB_CONFIG_FROM_YAML`).
- Do not touch `tests/databricks/`, and do not modify anything existing
  under `tests/unit/` — this plan only *adds* one new file there (Task 2).
  Everything else is additive: under `tests/local/` (Tasks 1/3/5), or in a
  new sibling directory `tests/config/` (Task 4). The only shared file
  touched by more than one task is `framework/pyproject.toml` (Task 4 adds
  a pytest marker).
- New silver job YAML must validate against `SilverOptions`
  (`framework/fabricks/models/job.py:85`): `mode` ∈ `AllowedModesSilver`,
  `change_data_capture` ∈ `AllowedChangeDataCaptures`, `parents: list[str]`.
- New bronze job YAML must validate against `BronzeOptions`
  (`framework/fabricks/models/job.py:66`): `mode` ∈ `AllowedModesBronze`
  (this plan only uses `"register"`), `uri: str` (mandatory),
  `keys: list[str] | None`.
- New step YAML must validate against `SilverConf`/`SilverOptions`
  (`framework/fabricks/models/step.py:55,89`): `options.parent: str`
  (mandatory), `options.order: int` (mandatory).

---

### Task 1: Turn on YAML-driven job config resolution for the local suite

**Files:**
- Modify: `framework/tests/local/conftest.py:29-31`
- Test: `framework/tests/local/test_config_loads.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `fabricks.context.IS_JOB_CONFIG_FROM_YAML == True` for every test
  under `tests/local/`. Task 3/4 rely on this being set before they run
  `get_step`/`get_job` — Task 2 (a pure YAML/pydantic check, no
  `fabricks.context` import at all) doesn't need it.

- [ ] **Step 1: Write the failing test**

Append to `framework/tests/local/test_config_loads.py`:

```python
def test_job_config_from_yaml_is_enabled():
    from fabricks.context import IS_JOB_CONFIG_FROM_YAML

    assert IS_JOB_CONFIG_FROM_YAML is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `podman compose -f framework/tests/local/docker-compose.yml run --rm local-tests pytest tests/local/test_config_loads.py::test_job_config_from_yaml_is_enabled -v`
(or the project's equivalent local-test invocation — see
`docs/TEST.md`/`docs/adr/0001-duckdb-backend-for-local-cdc-tests.md` for the
exact command this repo uses to run `tests/local/`)
Expected: FAIL — `IS_JOB_CONFIG_FROM_YAML` is `False` (default in
`framework/fabricks/models/config/models.py:45`).

- [ ] **Step 3: Set the env var in conftest.py**

In `framework/tests/local/conftest.py`, right next to the existing env-var
block (line 29-31):

```python
os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"
```

- [ ] **Step 4: Run test to verify it passes**

Run: same command as Step 2.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add framework/tests/local/conftest.py framework/tests/local/test_config_loads.py
git commit -m "test: enable YAML-driven job config resolution for local suite"
```

---

### Task 2: Fast unit coverage for the new YAML — no Spark, no container

**Why this has to be a separate, `tests/unit/`-tier task:** importing the
*real* `fabricks.context` package always builds a real `SparkSession` as a
side effect of import — `SPARK = build_spark_session(app_name="default")`
runs unconditionally at `framework/fabricks/context/spark_session.py:83`,
and `build_spark_session` immediately issues several `spark.sql(...)`
calls. There is no way to import `fabricks.context` — and therefore no way
to call the real `get_step()`/`get_job()`, which both import it — without
paying for a Spark session. That's exactly why `tests/unit/conftest.py`
replaces `sys.modules["fabricks.context"]` with a `MagicMock` wholesale
(`framework/tests/unit/conftest.py:28`): it avoids Spark, but it also means
`Bronzes`/`Silvers`/`STEPS` become mock attributes, not real parsed YAML —
so `get_job`/`get_step` themselves can't be meaningfully exercised under
that mock; only genuinely Spark-free code can.

What *is* genuinely Spark-free: `fabricks.models` (confirmed — nothing in
`framework/fabricks/models/` imports `fabricks.context`). The pydantic
validation Task 3's new YAML files must pass (`JobConfBronze`,
`JobConfSilver`, `SilverConf`/`SilverOptions` — see Global Constraints) can
be exercised directly against the parsed YAML dict, with no Spark session,
no container, no `podman compose` — same tier as `tests/unit/test_read_yaml.py`.
This won't prove `get_step`/`get_job` resolve them (that's still Task 4,
still needs the container) — it catches config-shape mistakes (wrong mode
literal, missing mandatory field, typo'd key) in seconds, before ever
starting the slow local suite.

**Files:**
- Create: `framework/tests/unit/test_local_runtime_job_yaml.py`

**Interfaces:**
- Consumes: `yaml` (stdlib PyYAML, already a fabricks dependency),
  `fabricks.models.JobConfBronze`/`JobConfSilver`
  (`framework/fabricks/models/job.py:151,158`), `fabricks.models.step.SilverConf`
  (`framework/fabricks/models/step.py:89`) — none of these trigger
  `fabricks.context`. Reads the same YAML files Task 3 creates
  (`framework/tests/local/runtime/bronze/_config.kings.yml`, `.../queens.yml`,
  `framework/tests/local/runtime/silver/_config.kings_and_queens.yml`) plus
  the new `bronze`/`silver` step blocks Task 3 adds to
  `framework/tests/local/runtime/fabricks/conf.fabricks.yml`.
- Produces: nothing further tasks depend on — an independent, fast
  correctness check that runs in `tests/unit/`, not `tests/local/`.

- [ ] **Step 1: Write the failing test**

Create `framework/tests/unit/test_local_runtime_job_yaml.py`:

```python
from pathlib import Path

import yaml

from fabricks.models import JobConfBronze, JobConfSilver
from fabricks.models.step import SilverConf

_RUNTIME = Path(__file__).resolve().parents[1] / "local" / "runtime"


def _load_jobs(path: Path) -> list[dict]:
    return [d["job"] for d in yaml.safe_load(path.read_text())]


def test_bronze_king_job_yaml_is_valid_register_mode():
    jobs = _load_jobs(_RUNTIME / "bronze" / "_config.kings.yml")
    assert len(jobs) == 1

    conf = JobConfBronze.model_validate({**jobs[0], "step": "bronze"})
    assert conf.options.mode == "register"
    assert conf.options.uri == "/workspace/tests/local/.storage/bronze_external/king"
    assert conf.options.keys == ["id"]


def test_bronze_queen_job_yaml_is_valid_register_mode():
    jobs = _load_jobs(_RUNTIME / "bronze" / "_config.queens.yml")
    assert len(jobs) == 1

    conf = JobConfBronze.model_validate({**jobs[0], "step": "bronze"})
    assert conf.options.mode == "register"
    assert conf.options.uri == "/workspace/tests/local/.storage/bronze_external/queen"


def test_silver_king_and_queen_job_yaml_is_valid():
    jobs = _load_jobs(_RUNTIME / "silver" / "_config.kings_and_queens.yml")
    assert len(jobs) == 1

    conf = JobConfSilver.model_validate({**jobs[0], "step": "silver"})
    assert conf.options.mode == "update"
    assert conf.options.change_data_capture == "scd1"
    assert conf.options.parents == ["bronze.queen_scd1", "bronze.king_scd1"]


def test_silver_step_yaml_declares_bronze_parent():
    data = yaml.safe_load((_RUNTIME / "fabricks" / "conf.fabricks.yml").read_text())
    conf = next(d["conf"] for d in data)
    silver_step = next(s for s in conf["silver"] if s["name"] == "silver")

    step_conf = SilverConf.model_validate(silver_step)
    assert step_conf.options.parent == "bronze"
    assert step_conf.path_options.runtime == "silver"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest framework/tests/unit/test_local_runtime_job_yaml.py -v`
(plain `pytest`, no `podman compose` — this is the whole point of this
task) Expected: FAIL — `framework/tests/local/runtime/bronze/_config.kings.yml`
etc. don't exist yet (write this task before or after Task 3's YAML files
exist; either order works, but the test can only pass once they do).

- [ ] **Step 3: Run test to verify it passes**

Once Task 3's YAML files exist, run: same command as Step 2.
Expected: PASS — 4 tests green, in well under a second, no container
startup.

- [ ] **Step 4: Commit**

```bash
git add framework/tests/unit/test_local_runtime_job_yaml.py
git commit -m "test: validate local bronze/silver job YAML against fabricks models (unit, no container)"
```

---

### Task 3: Add `bronze`/`silver` steps + job YAML files to `tests/local/runtime/`

**`register` mode, not `append`:** `tests/databricks/runtime/bronze/_config.kings.yml`'s
`mode: append` reads raw files from a cloud `uri` through a parser and (per
`Bronze.stream` — `framework/fabricks/core/jobs/bronze.py:35-36`, `return
self.mode not in ["register"]`) defaults to *streaming* — neither the
cloud URI nor a streaming read is exercisable outside Databricks.
`mode: register` is the one bronze mode built for exactly this: no parser
(`Bronze.parser` asserts `self.mode not in ["register"]`, line 140), no
stream (`stream` is `False` for it), and it reads directly from an
existing Delta table at `uri` via `select * from delta.\`{uri}\`` (`Bronze.parse`,
line 157-171). That table just needs to exist on disk — exactly the kind
of state `tests/local/conftest.py` already builds by hand for `NoCDC`.

**Files:**
- Modify: `framework/tests/local/runtime/fabricks/conf.fabricks.yml`
- Create: `framework/tests/local/runtime/bronze/_config.kings.yml`
- Create: `framework/tests/local/runtime/bronze/_config.queens.yml`
- Create: `framework/tests/local/runtime/silver/_config.kings_and_queens.yml`
- Modify: `framework/tests/local/conftest.py` (seed the two Delta tables
  the `register`-mode jobs read from)

**Interfaces:**
- Consumes: `IS_JOB_CONFIG_FROM_YAML` from Task 1 (must be `True` for these
  steps' job configs to ever be reached by `get_job_conf`/`get_jobs_internal_df`
  — with it `False`, both fall back to `select * from fabricks.<step>_jobs`,
  a catalog table this plan never populates). Also consumes the existing
  `local_spark` fixture (`tests/local/conftest.py:92`) and fixture files
  `tests/local/fixtures/job1/bronze_{king,queen}_scd1.jsonl` (already read
  by `king_and_queen_built`, 6 rows each).
- Produces: a `bronze` step (jobs `topic="king"`/`"queen"`, `item="scd1"`,
  `mode="register"`) and a `silver` step (job `topic="king_and_queen"`,
  `item="scd1"`), both resolvable via `fabricks.context.STEPS`/`Bronzes`/
  `Silvers` and via `get_job_conf`/`get_jobs_internal_df` — plus two real
  Delta tables on disk at `/workspace/tests/local/.storage/bronze_external/king`
  and `.../queen` that the `bronze.king_scd1`/`bronze.queen_scd1` jobs'
  `uri` points at. Task 4's tests consume all of this: the king-only/
  queen-only tests hit
  the `bronze` step directly (`bronze.king_scd1`/`bronze.queen_scd1` are
  exactly the `parents` the `king_and_queen` silver job lists below), not
  a separate silver topic — fabricks has no single-entity silver job in
  this fixture set, only the combined `king_and_queen` one.

- [ ] **Step 1: Add the `bronze:`/`silver:` blocks to `conf.fabricks.yml`**

Add these keys (sibling to the existing top-level `path_options`/
`databases` keys) to `framework/tests/local/runtime/fabricks/conf.fabricks.yml`:

```yaml
    bronze:
      - name: bronze
        path_options:
          storage: /workspace/tests/local/.storage/bronze
          runtime: bronze
        options:
          order: 101
    silver:
      - name: silver
        path_options:
          storage: /workspace/tests/local/.storage/silver
          runtime: silver
        options:
          order: 201
          parent: bronze
```

`runtime: bronze`/`runtime: silver` resolve relative to the runtime
config's own directory (`tests/local/runtime/`, i.e.
`RuntimeConf.config.resolved_paths.runtime` — see
`framework/fabricks/models/runtime/utils.py:145`), landing on
`tests/local/runtime/bronze/`/`tests/local/runtime/silver/` — mirroring
`tests/databricks/runtime/bronze|silver/`'s layout exactly.
`path_options.storage` reuses the same `/workspace/tests/local/.storage/...`
absolute-path convention as the file's existing `databases:` block (see the
long comment there on why relative paths break local Delta merges) — these
steps' storage paths don't need to be the *same* directory as the `bronze`/
`silver` *database*'s storage declared above, but they do need to be
absolute for the same reason.

- [ ] **Step 2: Create the bronze job YAML files (register mode)**

Create `framework/tests/local/runtime/bronze/_config.kings.yml` — `uri`
points at a fixed local Delta table path this task seeds in Step 4 below,
distinct from the job's own eventual output table
(`PATHS_STORAGE["bronze"]/king/scd1`) so "the external source" and "the
job's registered table" stay two different things, matching how `register`
mode is used for real (see `tests/databricks/runtime/bronze/_config.monarchs.yml`'s
`delta`/`memory` items, which read from `.../raw/delta/monarch`, not from
`bronze.monarch_delta`'s own path):

```yaml
---
- job:
    step: bronze
    topic: king
    item: scd1
    tags: [test]
    options:
      mode: register
      uri: /workspace/tests/local/.storage/bronze_external/king
      keys: [id]
```

Create `framework/tests/local/runtime/bronze/_config.queens.yml`:

```yaml
---
- job:
    step: bronze
    topic: queen
    item: scd1
    tags: [test]
    options:
      mode: register
      uri: /workspace/tests/local/.storage/bronze_external/queen
      keys: [id]
```

Only `scd1` items — `king_and_queen`'s silver `scd1` job (Step 3 below)
only depends on `bronze.king_scd1`/`bronze.queen_scd1`; an `scd2` item
would need its own separate external table and isn't needed to prove
`get_job`/`get_step` work locally.

- [ ] **Step 3: Create the silver job YAML file**

Create `framework/tests/local/runtime/silver/_config.kings_and_queens.yml`
— same shape as `tests/databricks/runtime/silver/_config.kings_and_queens.yml`,
trimmed to the `scd1` item this task actually exercises:

```yaml
- job:
    step: silver
    topic: king_and_queen
    item: scd1
    tags: [test]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.queen_scd1, bronze.king_scd1]
```

- [ ] **Step 4: Seed the two external Delta tables `register` mode reads from**

In `framework/tests/local/conftest.py`, add a new session-scoped fixture
next to `king_and_queen_built` (uses the same NDJSON fixture files that
fixture already reads, so no new fixture data):

```python
@pytest.fixture(scope="session")
def king_and_queen_registered_sources(local_spark):
    """Seeds the two Delta tables tests/local/runtime/bronze/_config.{kings,queens}.yml's
    `register`-mode jobs read from (their `uri`), independent of anything
    `king_and_queen_built` creates under its own `king_and_queen_step_N`
    tables. Session-scoped + written once: `register` mode's source table
    is read-only from the job's point of view.
    """
    from fabricks.utils.path import FileSharePath

    fixtures_root = Path(__file__).resolve().parent / "fixtures" / "job1"
    for entity in ("king", "queen"):
        path = FileSharePath.from_uri(f"/workspace/tests/local/.storage/bronze_external/{entity}")
        df = local_spark.read.json(str(fixtures_root / f"bronze_{entity}_scd1.jsonl"))
        df.write.format("delta").mode("overwrite").save(path.string)
```

- [ ] **Step 5: Commit**

```bash
git add framework/tests/local/runtime/fabricks/conf.fabricks.yml framework/tests/local/runtime/bronze/_config.kings.yml framework/tests/local/runtime/bronze/_config.queens.yml framework/tests/local/runtime/silver/_config.kings_and_queens.yml framework/tests/local/conftest.py
git commit -m "test: add local bronze king/queen (register mode) + silver king_and_queen job configs"
```

(No test yet — Task 5 is what exercises this. Committing config-only
changes here keeps this task's diff independently reviewable, per this
plan's task-sizing: config addition and its proof are two separate
reviewer-facing units.)

---

### Task 4: A third test tier — `tests/config/` — real `get_step`/`get_job`, no container, no Java

**Why this needs its own directory, not a file under `tests/unit/`:**
`tests/unit/conftest.py` mocks `sys.modules["fabricks.context"]` wholesale,
unconditionally, for *every* test under `tests/unit/` — pytest collects a
parent directory's `conftest.py` before any subdirectory's, so a new file
placed under `tests/unit/` can't "undo" that mock in time to get real
`STEPS`/`Bronzes`/`Silvers`. This plan wants exactly the opposite of that
mock: real config resolution, fake Spark. That has to live somewhere
`fabricks.context` is never globally mocked away — a new sibling of
`tests/unit/`/`tests/local/`/`tests/databricks/`.

**What actually needs faking, and why `fabricks.utils.spark`'s mock (the
one `tests/unit/` uses) doesn't reach it:** confirmed by reading
`framework/fabricks/context/spark_session.py:83`,
`SPARK = build_spark_session(app_name="default")` runs at
`fabricks.context` import time, and `build_spark_session`'s default path
calls `SparkSession.builder.appName(...).enableHiveSupport().getOrCreate()`
**directly** — it never calls `fabricks.utils.spark.get_spark()` (only the
deprecated `init_spark_session` does). So the seam to patch is
`pyspark.sql.SparkSession.builder` itself, confirmed via introspection to
be a custom `classproperty` descriptor
(`type(SparkSession.__dict__["builder"])` →
`pyspark.sql.session.classproperty`) — replacing it with a plain
`MagicMock` on the class works (regular attribute assignment shadows the
descriptor). Also confirmed empirically in this environment: no `java` on
`PATH` at all, so even a bare non-Delta `SparkSession.getOrCreate()` would
fail outright here — patching `SparkSession.builder` is not an
optimization, it's the only way this tier runs on a plain dev host.

**A second, independent eager-Spark path had to be defused too — this is
the correction to the plan, found empirically while implementing it:**
`framework/fabricks/utils/spark.py` does `spark = get_spark()` **at its own
module import time**, reached transitively via `fabricks.context.
spark_session` → `fabricks.context.secret` → `fabricks.utils.spark`. And
`get_spark()` does `from delta import configure_spark_with_delta_pip` —
which additionally failed in this venv for an unrelated reason (see
"Delivered this session" above: the installed `delta-spark` was missing
files). Patching `SparkSession.builder` alone doesn't reach this path —
`fabricks.utils.spark`'s module body never gets that far. The fix: replace
`sys.modules["fabricks.utils.spark"]` with a `MagicMock` before anything
imports it — the same seam `tests/unit/conftest.py` already uses, just
scoped to that one module instead of all of `fabricks.context`.

**Fragility this accepts:** if `framework/fabricks/context/spark_session.py`
is ever refactored to build `SPARK` a different way (e.g. routing through
`get_spark()` after all), this tier's `conftest.py` silently stops faking
Spark and either errors (no Java) or builds a real session — there is no
compile-time link between the patch and what it's patching. Worth it here
because the payoff is `get_step`/`get_job` running in well under a second
with no `podman compose`.

**Files (as actually built):**
- Create: `framework/tests/config/__init__.py` (empty)
- Create: `framework/tests/config/conftest.py`
- Create: `framework/tests/config/test_get_job.py` (not yet written — needs
  Task 3's `bronze`/`silver` YAML; see Status)
- Create: `framework/tests/config/test_ddl_option_mapping.py` (done — see
  "Delivered this session", doesn't need Task 3)
- Create: `framework/tests/config/test_option_hierarchy.py` (done — see
  "Delivered this session", needed its own small `semantic`-step YAML
  addition instead of Task 3's)
- Modify: `framework/pyproject.toml` (register the `config` pytest marker
  — done)

**Interfaces:**
- Consumes: Task 3's `bronze`/`silver` steps + job YAML (reuses
  `framework/tests/local/runtime/fabricks/conf.fabricks.yml` as-is — same
  `FABRICKS_CONFIG` value `tests/local/runtime/fabricks/conf.fabricks.yml`
  Task 1 sets, so this tier needs no config file of its own).
- Produces: `test_get_step_silver_resolves_locally`/
  `test_get_job_silver_king_and_queen_scd1_resolves_locally` and two new
  bronze checks — the non-dataframe-reading subset of what was Task 4's
  test file before this task existed. Task 5 keeps only the two tests that
  read real Delta state (`.parse()`), which this fake-Spark tier can't do.

- [x] **Step 1: Write the conftest — done, with the two-patch fix**

`framework/tests/config/conftest.py` (as actually committed to the working
tree):

```python
"""Conftest for the config-resolution tier: real fabricks.context/get_step/
get_job against real YAML, with Spark faked out so no JVM (and therefore no
Java installation) is needed. See docs/superpowers/plans/
2026-09-03-local-get-job-get-step.md, Task 4, for the design.

Two independent real-Spark-construction paths have to be defused, not one:

1. fabricks/context/spark_session.py's `SPARK = build_spark_session(...)`
   calls `pyspark.sql.SparkSession.builder...getOrCreate()` directly - so
   that classproperty is patched to a MagicMock below.
2. fabricks/utils/spark.py does `spark = get_spark()` at ITS OWN import
   time (reached transitively via fabricks.context.spark_session ->
   fabricks.context.secret -> fabricks.utils.spark), and get_spark() does
   `from delta import configure_spark_with_delta_pip` - which fails in
   this venv (delta-spark's `delta/` package here has no `__init__.py`,
   just a namespace-package stub) regardless of Java. Patching
   SparkSession.builder doesn't reach this - fabricks.utils.spark's own
   module body never gets that far. So the whole module is replaced in
   sys.modules first, the same seam tests/unit/conftest.py uses - but
   WITHOUT also replacing fabricks.context itself, since this tier wants
   fabricks.context's real STEPS/CONF_RUNTIME parsing to run.

IMPORTANT: same import-order rule as tests/local/conftest.py - the env
vars and both patches below must execute before fabricks.context is ever
imported (by this conftest or by any test file), since fabricks.context.
SPARK is built once, at that first import, and cached at module level.

Do NOT mix tests/config with tests/unit (or tests/local/tests/databricks)
in the same pytest invocation, for the same reason: tests/unit/conftest.py
replaces sys.modules["fabricks.context"] with a MagicMock outright, and
whichever conftest's module body runs first wins for the whole process -
this tier needs the real fabricks.context, tests/unit needs the fake one.
Confirmed empirically: `pytest tests/unit tests/config` in one run fails
collecting tests/config with `ModuleNotFoundError: fabricks.context is not
a package`, because tests/unit's mock (collected first) already replaced
it. Run each tier as its own separate pytest invocation.
"""

import os
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[2]

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

_fake_spark_session = MagicMock(name="fake_spark_session")
_fake_dbutils = MagicMock(name="fake_dbutils")

sys.modules["fabricks.utils.spark"] = MagicMock(
    spark=_fake_spark_session,
    dbutils=_fake_dbutils,
    get_spark=MagicMock(return_value=_fake_spark_session),
    get_dbutils=MagicMock(return_value=_fake_dbutils),
)

from pyspark.sql import SparkSession  # noqa: E402 - must follow the setup above

_fake_builder = MagicMock(name="fake_spark_session_builder")
_fake_builder.appName.return_value = _fake_builder
_fake_builder.config.return_value = _fake_builder
_fake_builder.enableHiveSupport.return_value = _fake_builder
_fake_builder.getOrCreate.return_value = _fake_spark_session
SparkSession.builder = _fake_builder


def pytest_collection_modifyitems(items):
    """Automatically add 'config' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        if Path(item.fspath).is_relative_to(root):
            item.add_marker(pytest.mark.config)
```

Note the path math: `parents[2]` from `tests/config/conftest.py` (parents[0]
= `tests/config`, parents[1] = `tests`, parents[2] = `framework`) — the
original plan text had this at `parents[1]`, an off-by-one caught by
actually running it (`FileNotFoundError` on a doubled `tests/tests/...`
path).

- [x] **Step 2: Register the `config` marker — done**

In `framework/pyproject.toml`'s `[tool.pytest.ini_options]` (around line
122), add a fourth line to the existing `markers` list:

```toml
markers = [
    "unit: marks tests as unit tests (no spark dependencies)",
    "databricks: marks tests as Databricks-cluster integration tests (slower, needs a live cluster)",
    "local: marks tests as local CDC/DDL tests (real local Spark+Delta, no Databricks)",
    "config: marks tests as config-resolution tests (real get_step/get_job, fake Spark, no container)",
]
```

- [x] **Step 3: Create the empty package marker — done**

`framework/tests/config/__init__.py` (empty file — no content).

- [ ] **Step 4: Write the still-pending test — blocked on Task 3**

Not yet written (needs Task 3's `bronze`/`silver` `king`/`queen` YAML to
exist first). Once Task 3 is done, create
`framework/tests/config/test_get_job.py`:

```python
from fabricks.core import get_job, get_step


def test_get_step_silver_resolves_locally():
    step = get_step("silver")
    assert step.name == "silver"
    assert step.options.parent == "bronze"


def test_get_job_silver_king_and_queen_scd1_resolves_locally():
    job = get_job(step="silver", topic="king_and_queen", item="scd1")
    assert job.options.mode == "update"
    assert job.options.change_data_capture == "scd1"
    assert job.options.parents == ["bronze.queen_scd1", "bronze.king_scd1"]


def test_get_job_bronze_king_only_is_register_mode_no_streaming():
    job = get_job(step="bronze", topic="king", item="scd1")
    assert job.options.mode == "register"
    assert job.stream is False, "register mode must disable streaming"


def test_get_job_bronze_queen_only_is_register_mode_no_streaming():
    job = get_job(step="bronze", topic="queen", item="scd1")
    assert job.options.mode == "register"
    assert job.stream is False, "register mode must disable streaming"
```

No fixtures needed — unlike `tests/local/`'s versions of these same
assertions, there's no real Spark session to inject and no
`king_and_queen_registered_sources` table to seed, since none of these
four tests read a dataframe.

- [ ] **Step 5: Run test to verify it fails**

Run: `pytest framework/tests/config/test_get_job.py -v`
(plain `pytest`, no `podman compose`, no Java required)
Expected: FAIL — before Task 3's YAML files exist, `silver`/`bronze` step
or job not found.

- [ ] **Step 6: Run test to verify it passes**

Once Task 3's YAML files exist, run: same command as Step 5.
Expected: PASS — all four tests green, no container startup, well under a
second once the venv's already warm.

- [ ] **Step 7: Commit**

Note: `test_ddl_option_mapping.py` and `test_option_hierarchy.py` (plus
the `semantic`-step YAML their tests need) are already sitting in the
working tree, delivered ahead of this step — either fold them into this
same commit, or commit them separately first; either is fine, they don't
depend on `test_get_job.py`.

```bash
git add framework/tests/config/ framework/pyproject.toml
git commit -m "test: add config-resolution tier — real get_step/get_job, fake Spark, no container"
```

---

### Task 5: Prove `get_job`'s `register`-mode read against real local state

**Files:**
- Create: `framework/tests/local/test_get_job.py`

**Interfaces:**
- Consumes: `fabricks.core.get_job`
  (`framework/fabricks/core/jobs/get_job.py:28`), Task 3's `bronze` steps +
  job YAML + `king_and_queen_registered_sources` fixture. Task 4 already
  covers the non-dataframe assertions (mode, `stream`, options shape) for
  the same two jobs — this task is only the part Task 4's fake-Spark tier
  structurally cannot do: reading a real Delta table.
- Produces: nothing further tasks depend on — this is the plan's proof
  point that `get_job` drives real local state, not just config parsing.
  `test_dependency.py`/`test_step.py` migration is separate, follow-on
  work (see Global Constraints — out of this plan's scope).

- [ ] **Step 1: Write the failing test**

Create `framework/tests/local/test_get_job.py`:

```python
from fabricks.core import get_job


def test_get_job_bronze_king_only_reads_real_registered_delta_table(
    local_spark, king_and_queen_registered_sources
):
    job = get_job(step="bronze", topic="king", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from bronze_king_scd1.jsonl"
    assert "id" in df.columns


def test_get_job_bronze_queen_only_reads_real_registered_delta_table(
    local_spark, king_and_queen_registered_sources
):
    job = get_job(step="bronze", topic="queen", item="scd1")

    df = job.parse(stream=False)
    assert df.count() == 6, "expected all 6 rows from bronze_queen_scd1.jsonl"
    assert "id" in df.columns
```

`job.parse(stream=False)` runs `Bronze`'s real `mode == "register"` branch
(`framework/fabricks/core/jobs/bronze.py:157-171`), which does
`self.spark.sql(f"select * from {self}")` against `job.data_path` — i.e.
it reads back the exact Delta table `king_and_queen_registered_sources`
wrote in Task 3, through the exact same `get_job()`-resolved object the
Databricks suite would use, not a hand-built `NoCDC`/`SCD1` object like
`king_and_queen_built` uses.

- [ ] **Step 2: Run test to verify it fails**

Run: `podman compose -f framework/tests/local/docker-compose.yml run --rm local-tests pytest tests/local/test_get_job.py -v`
Expected: FAIL — before Task 3's config exists (if running tasks out of
order, confirm Task 1+3 are done first, since this task has no code
changes of its own to make it pass).

- [ ] **Step 3: Run test to verify it passes**

Run: same command as Step 2.
Expected: PASS — both tests green.

- [ ] **Step 4: Commit**

```bash
git add framework/tests/local/test_get_job.py
git commit -m "test: prove get_job's register-mode read against real local Delta state"
```

---

## Out of scope (follow-up work, not this plan)

- Actually **running the silver job** via `get_job(step="silver", ...).run()`
  (needed to migrate `test_dependency.py`/`test_silver.py` off the
  hand-built `NoCDC`/`SCD1`/`SCD2` objects `king_and_queen_built` uses).
  Task 4/5 prove the *bronze* `register`-mode read round-trips through a
  real `get_job()` object, but the *silver* `king_and_queen` job's own
  `.run()` path pulls from its bronze parents by their registered table
  name (`bronze.king_scd1`/`bronze.queen_scd1`), not from the
  `bronze_external/{king,queen}` source tables directly — that requires
  first running the bronze jobs themselves (`job.run()`, not just
  `job.parse()`) so they persist their own `bronze.king_scd1`/
  `bronze.queen_scd1` tables, then reconciling that with what
  `king_and_queen_built` already builds by hand for the same tables.
- Also **running** `job.update_dependencies()` locally (needed for
  `test_dependency.py`'s dependency-graph assertions specifically) —
  independent follow-up from the above.
- **`Bronze`/`Silver`/`Gold.get_dependencies()`, at the `tests/config/`
  tier** (real job object, no Spark call needed — same shape as
  `test_option_hierarchy.py`): Gold's SQL-parsing half is unit-tested
  (`test_sql_dependencies.py`), but the `__current`-stripping/dedup glue in
  `Gold.get_dependencies()` itself (`framework/fabricks/core/jobs/gold.py:183`)
  isn't. Silver has no SQL parsing at all — `Silver.get_dependencies()`
  (`framework/fabricks/core/jobs/silver.py:172`) only has two branches:
  explicit `parents` (echoed back) or, when `parents` is omitted, a
  naming-convention fallback `f"{parent_step}.{topic}_{item}"` — untested
  by anything built so far, since every local job so far declares
  `parents` explicitly. Would need one job config that omits `parents`.
- **`Step._get_dependencies_internal()`** (`framework/fabricks/core/steps/
  base.py:162`) — aggregates every job's own `get_dependencies()` across a
  step via `run_in_parallel`. Also a `tests/config/`-tier candidate (real
  job objects, no catalog table needed for this part), separate from
  `Step.update_dependencies()`/`update_configurations()` below, which
  *does* need real catalog tables.
- **`Gold.get_cdc_context()`** (`framework/fabricks/core/jobs/gold.py:243`)
  — a pure dict-building decision function (soft_delete derivation,
  forced deduplicate/rectify, add_key/add_hash from column presence,
  mode=="memory" override), same `tests/config/` shape as
  `test_column_selection.py`. Deliberately **not** built this session:
  `mode`/`change_data_capture` are `cached_property` on the job object (see
  `framework/fabricks/core/jobs/base/configurator.py:256,286`), so varying
  them per test case can't reuse the same `job.conf.model_copy(...)` trick
  `test_column_selection.py` uses for `table_options` (a plain `@property`,
  re-read fresh each call) — it would need either fresh job YAML per
  mode/cdc combination, or constructing a fresh `get_job(...)` call per
  scenario before either cached property is first touched. Worth doing,
  just a slightly different shape than what's here.
- Explicitly **not** duplicating `test_option_hierarchy.py` for `masks`/
  `powerbi` (the other two `_get_option_hierarchy` call sites besides
  `properties` — confirmed by grep, there are no others): the function has
  no per-attribute branching, so a `masks` version would execute the exact
  same three lines as the `properties` version already does — no test can
  fail there that the existing one wouldn't also catch. What *does* differ
  per-attribute is the caller logic around each one (`create_table()`'s
  `default_properties` three-way selection keyed on `maximum_compatibility`/
  `powerbi`, and `powerbi` unconditionally suppressing `properties`) — that
  caller logic, not the hierarchy mechanism itself, is the next real
  candidate if more `tests/config/` coverage is wanted here.
- **`DagGenerator.get_dependencies()`** (`framework/fabricks/core/dags/
  generator.py:54`) — confirmed **Databricks-only, not a local candidate at
  all**: it queries `fabricks.dependencies`/`fabricks.jobs`/
  `fabricks.dependencies_circular` (real catalog state from a prior
  `update_dependencies()` run) and `DagGenerator.generate()` around it
  creates real Azure Queues. No local harness should try to fake either.
- Migrating `test_step.py`'s `update_dependencies`/`update_views_list`/
  `create_db_objects`/`update_configurations` calls — these write to
  `fabricks.*` catalog tables regardless of `IS_JOB_CONFIG_FROM_YAML`
  (see `BaseStep.update_configurations` in
  `framework/fabricks/core/steps/base.py:405`), so they need the `fabricks`
  database Task's `Database(...).create()` loop in `conftest.py` extended
  to also create `fabricks`, not just `bronze/silver/gold/expected`.
- Migrating `test_overwrite.py`/`test_semantic.py`'s property assertions —
  independent of `get_job`/`get_step` working; can proceed in parallel
  once someone picks it up (see this session's earlier analysis: OSS
  Delta 4.1 supports `identityColumns`/`columnMapping`/`TBLPROPERTIES`
  identically to Databricks Runtime for these).

---

## Appendix: full job inventory (`tests/databricks/runtime/`)

Every `- job:` entry across `bronze/`, `silver/`, `gold/`, `gold/transf/`,
`semantic/`, verdict per job. Legend:

- **CONFIG** — pure decision/mapping logic on a real job/step object with
  Spark faked out (the `tests/config/` tier — no container, no Java): DDL
  option mapping, dependency parsing, option-hierarchy precedence,
  column-selection, check comparison logic. Added mid-session once this
  tier proved out further than originally scoped — see "Delivered this
  session". **(done)** = actually built and passing; **CONFIG\*** =
  identified as this shape but not yet built.
- **LOCAL** — needs real Spark+Delta *data* (an actual materialized table,
  not just DDL/config), but no cloud file, no notebook, no Databricks
  Runtime feature.
- **LOCAL\*** — needs the same rework this plan already does for bronze
  `king`/`queen`: swap a `abfss://` `uri` for a local Delta path + seed
  fixture data (register-mode jobs), or swap a raw-file `parser` read for
  local fixture files (memory/append-mode bronze jobs), or swap `dbutils`
  for `pathlib` (one semantic job).
- **DATABRICKS** — genuinely needs a real notebook (`notebook: true` or
  `invoker_options` referencing one) or real wall-clock multi-job
  scheduling; no local rework fixes this.
- **LOCAL / DATABRICKS** — the job's *logic* is local-testable, but the
  *test* asserting it today reads a real schedule's error/status log, an
  integration concern separate from the job itself (see `test_check.py`
  discussion earlier this session).

### `bronze/`

| Job | Mode | Verdict | Why |
|---|---|---|---|
| `bronze.king_scd1`, `bronze.king_scd2` | append | LOCAL\* | raw file + `monarch` parser from `abfss://` — same rework this plan already does (register substitute) |
| `bronze.queen_scd1`, `bronze.queen_scd2` | append | LOCAL\* | same as king |
| `bronze.memory_scd1`, `bronze.memory_scd2` | append | LOCAL\* | same |
| `bronze.monarch_scd1`, `bronze.monarch_scd2` | memory | LOCAL\* | parser + cloud file, non-persisted result |
| `bronze.monarch_delta`, `bronze.monarch_memory` | register | LOCAL | already register mode — only needs `uri` pointed at a local Delta path + seed, exactly this plan's Task 2 pattern |
| `bronze.prince_special_char`, `bronze.prince_deletelog` | memory | LOCAL\* | parser + cloud file |
| `bronze.princess_extend`/`manual`/`drop`/`encrypt`/`latest`/`append`/`schema_drift`/`type_widening`/`order_duplicate`/`calculated_column`/`check`/`too_many_columns` (12 jobs) | memory | LOCAL\* | parser + cloud file |
| `bronze.princess_no_column` | register | LOCAL | already register mode |
| `bronze.regent_scd1`, `bronze.regent_scd2` | register | LOCAL | already register mode |

### `silver/`

Gated only by whether each job's **bronze parents** have local data (via
the register substitute above) — a silver job's own mode never needs cloud
storage or a notebook, since silver only reads already-registered bronze
tables.

| Job | Mode | Verdict | Why |
|---|---|---|---|
| `silver.king_and_queen_scd1`, `silver.king_and_queen_scd2` | update | LOCAL | this plan's Task 2/3 subject |
| `silver.memory_scd1`, `silver.memory_scd2` | memory | LOCAL | |
| `silver.monarch_scd1`, `silver.monarch_scd2`, `silver.monarch_delta` | update | LOCAL | |
| `silver.monarch_memory` | memory | LOCAL | |
| `silver.prince_special_char`, `silver.prince_deletelog`, `silver.prince_scd2` | update | LOCAL | |
| `silver.princess_extend`/`manual`/`encrypt`/`drop`/`schema_drift`/`type_widening`/`order_duplicate`/`calculated_column`/`check` (9 jobs) | update | LOCAL | |
| `silver.princess_latest`, `silver.princess_too_many_columns` | latest | LOCAL | |
| `silver.princess_append` | append | LOCAL | |
| `silver.princess_combine` | combine | LOCAL | |
| `silver.regent_scd1`, `silver.regent_scd2` | update | LOCAL | |

### `gold/gold/`

| Job | Mode | Verdict | Why |
|---|---|---|---|
| `gold.check_max_rows`/`min_rows`/`count_must_equal` | complete | **CONFIG (done)** | `Checker.check_post_run_extra()`'s comparison + exact error-message logic — `test_checker.py`, no container. What's genuinely left for Databricks: proving a real schedule actually invokes this check on a real job with real merged data. |
| `gold.check_time_ok`/`time_ko` | complete | **CONFIG (done)** | `Checker._check_run_time()`'s before/after window logic — `test_checker.py`. |
| `gold.check_fail`/`warning`/`skip`/`duplicate_key`/`duplicate_identity` (5 jobs) | complete | CONFIG\* | Same shape as the two rows above — `Checker._check()` (pre_run/post_run `.sql` file), `check_skip_run()`, `_check_duplicate_in_column()` all only touch Spark via `self.spark.sql(...).where(...).collect()`, controllable the same way. Not yet built — needs the `.pre_run.sql`/`.post_run.sql`/`.skip.sql` file each reads via `self.paths.to_runtime` to exist (or that path itself mocked) as well as the mocked query result. |
| `gold.dim_time` | memory | LOCAL | |
| `gold.dim_identity`, `gold.dim_overwrite` | update | LOCAL | identity columns — Delta 4.1 OSS supports them |
| `gold.dim_date` | complete | LOCAL | |
| `gold.fact_udf` | complete | LOCAL | UDF registration, plain Spark |
| `gold.fact_manual` | complete | LOCAL | |
| `gold.fact_notebook` | complete | DATABRICKS | `notebook: true` — job body is a notebook |
| `gold.fact_dependency_sql` | complete | LOCAL | dependency-graph metadata only |
| `gold.fact_dependency_notebook` | complete | DATABRICKS | `notebook: true` |
| `gold.fact_memory` | memory | LOCAL | |
| `gold.fact_option` | complete | **CONFIG (done)** | table properties/comment/cluster-by DDL — `test_ddl_option_mapping.py` |
| `gold.fact_order_duplicate`, `gold.fact_deduplicate`, `gold.fact_overwrite` | complete | LOCAL | |
| `gold.fact_append` | append | LOCAL | |
| `gold.fact_optimize_vacuum` | complete | LOCAL | OSS Delta supports `OPTIMIZE`/`VACUUM` — needs a real table to optimize, not just DDL |
| `gold.fact_no_drop` | memory | LOCAL | |
| `gold.fact_masker_and_commenter` | complete | **CONFIG (done)** | column masks/comments DDL — `test_ddl_option_mapping.py` |
| `gold.fact_foreign_keys`, `gold.fact_primary_key` | complete | **CONFIG (done)** | PK/FK constraint DDL — `test_ddl_option_mapping.py` (Delta 4.1 OSS supports both) |
| `gold.invoke_post_run`/`failed_pre_run`/`timedout_pre_run`/`notebook`/`notebooks`/`notebook_without_argument`/`timedout`/`notebooks_failed_pre_run`/`complete_invoker`/`complete_notebook` (10 jobs) | memory/invoke/complete | DATABRICKS | every one has `invoker_options` referencing a notebook or `notebook: true` |
| `gold.nocdc_update`, `gold.nocdc_deduplicate` | update | LOCAL | |
| `gold.scd0_update` | update | LOCAL | |
| `gold.scd1_update`/`last_timestamp`/`complete`/`memory`/`identity`/`special_char`/`script`/`generated_column`/`updated_column` (9 jobs) | update/complete/memory | LOCAL | |
| `gold.scd2_update`/`last_timestamp`/`complete`/`memory`/`correct_valid_from` (5 jobs) | update/complete/memory | LOCAL | |
| `gold.type_widening_merge`, `gold.type_widening_overwrite` | update/complete | LOCAL | |

### `gold/transf/`

| Job | Mode | Verdict | Why |
|---|---|---|---|
| `transf.fact_memory` | memory | LOCAL | |
| `transf.fact_register` | register | LOCAL\* | separate `register_options.uri` (not `options.uri`) — same local-Delta-seed rework |
| `transf.fact_wait_for` | complete | DATABRICKS | asserts real wall-clock ordering against `transf.fact_memory`'s actual run — needs a real multi-job schedule |
| `transf.fact_dummy` | complete | LOCAL | |
| `transf.fact_sample` | complete | DATABRICKS | `notebook: true` |

### `semantic/`

| Job | Mode | Verdict | Why |
|---|---|---|---|
| `semantic.dim_complex_query` | memory | LOCAL | |
| `semantic.fact_zstd` | complete | LOCAL\* | reads compression codec via `dbutils.fs.ls` — swap for `pathlib` against local storage |
| `semantic.fact_step_option`/`job_option` | complete | **CONFIG (done)** | `_get_option_hierarchy` precedence — `test_option_hierarchy.py`, no container |
| `semantic.fact_schema_drift`/`powerbi`/`table`/`clustering`/`partitioning` (5 jobs) | complete | LOCAL | table properties/partitions/clustering — needs real Delta *data*, not just DDL text |

**Totals (approximate):** ~90 jobs. **CONFIG** — pure decision/mapping
logic, no container, no Java (`gold.fact_option`/`masker_and_commenter`/
`foreign_keys`/`primary_key`'s DDL mapping, `gold.check_max_rows`/
`min_rows`/`count_must_equal`/`time_ok`/`time_ko`'s comparison logic,
`semantic.fact_step_option`/`job_option`'s hierarchy precedence, column
selection): ~8 jobs' worth **built** this session, another ~5 (the
remaining `gold.check_*` jobs) **identified but not yet built**. **LOCAL**
(needs real Spark+Delta *data*, not just DDL/config): the rest of the
previously-LOCAL rows, still real Spark+Delta candidates, ~55. **LOCAL\***
(needs a uri/parser/dbutils rework, same shape as this plan's Task 3):
~20. **DATABRICKS** (genuinely can't move — real notebook or real
multi-job wall-clock): ~15, concentrated in `gold.invoke_*` (10),
`*_notebook`/`notebook: true` jobs (4), and `transf.fact_wait_for`.
