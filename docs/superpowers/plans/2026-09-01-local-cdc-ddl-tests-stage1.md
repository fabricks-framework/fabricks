# Local CDC/DDL Tests — Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Naming update (2026-09-04):** `tests/unit/`, `tests/local/`, and
> `tests/databricks/` were renamed to `tests/plain/`, `tests/spark/apache/`,
> and `tests/spark/databricks/` (see [TEST.md](../../TEST.md)). Path
> references below are kept as originally written.

**Goal:** Get real, passing local CDC tests running against a local Spark + `delta-spark` session (no Databricks Connect, no cluster) — a Gold NoCDC smoke test with zero runtime-config dependency, and a Silver SCD1/SCD2 test covering every contiguous job1-through-job9 chain (9 scenarios, exercising both the initial bulk-load and incremental-merge-into-an-already-populated-table code paths), each driven by real fixture data and compared against the existing `expected/**/job*.sql` golden views.

**Architecture:** A new `LocalFileSharePath` sibling class (plain `pathlib`, no `dbutils`) plugs into the existing `resolve_fileshare_path()` factory by URI scheme (`abfss://` → `FileSharePath`, anything else → `LocalFileSharePath`). A minimal local runtime config (a single `conf.fabricks.yml`, no bronze/silver/gold step configs, no job-config YAML files at all) registers `bronze`/`silver`/`gold`/`expected` as plain `databases:` entries, pointed to via `FABRICKS_CONFIG`/`FABRICKS_BASE` env vars set at the top of `tests/local/conftest.py`, before any `fabricks` import in that process. A session-scoped pytest fixture builds one local, Delta-configured `SparkSession` and creates those databases against it. Both Bronze ingestion (Auto Loader) and the `Bronze`/`Silver`/`Gold` job-orchestration layer stay out of scope entirely — this plan calls the CDC layer directly (`NoCDC`, `SCD1`, `SCD2`), matching the pattern the existing `tests/databricks/jobs/job1/test_cdc.py` already uses (`NoCDC("gold", "nocdc", "overwrite")`), which needs no job config since `Configurator.__init__(database, *levels, change_data_capture, spark=None)` only resolves `Database(database)` — a storage-path lookup, not a job-config lookup. Fixture data is derived once per job (ported from `tests/databricks/utils.py`'s JSON→parquet/timestamp logic) into committed NDJSON, loaded directly into per-job bronze tables via `NoCDC(...).append(df)`, combined per job, and fed into `SCD1`/`SCD2`'s real `.update()` — the production merge-SQL-generation and execution path, unmodified — **once per job, sequentially**, so each job's merge runs against the table state the previous job's merge left behind (this exercises the incremental-merge code path a single-job scenario never touches; see Task 9's "Why job-sequential" note — the raw landing-file batch boundary within a job does *not* need this, only the job boundary does). `__key`/`__hash`/`__merge_key`/`__merge_condition` are computed internally from `keys=["id"]` (confirmed via `Processor.get_query_context()` — never caller-supplied). This plan's actual scope covers every contiguous prefix `[1]` through `[1..9]` (9 scenarios — the full range where both king and queen have plain, non-deletelog data); the mechanism is job-list-general so a future scenario starting elsewhere (e.g. `[2, 3]`, skipping job1) is a new list entry, not a redesign. A `[1, 2]`-and-beyond scenario merges against a table whose schema doesn't yet have job2's new `newField` column — handled via Delta's `autoMerge` (Task 4's `get_spark()`, matching production's own schema-drift handling), not new local-only reconciliation logic. Results are checked against the existing `expected/silver/{scd1,scd2}/job{01..09}.sql` reference views using the existing `compare.py` comparison logic. (Bypassing the parser/streaming-tied `Bronze` job class entirely also sidesteps that legacy abstraction outright, rather than needing to work around it.)

The Delta-configured `SparkSession` is built at `tests/local/conftest.py`'s *module import time* (not lazily inside the `local_spark` fixture body) and runs inside Docker, not on the host — see Task 7 for why both of these are load-bearing, not stylistic choices: a plain, non-Delta `SparkSession` gets built as a side effect of importing `fabricks.context` (transitively, e.g. via `from fabricks.cdc import NoCDC`) if anything wins that race first, and `SparkSession.builder.getOrCreate()` silently reuses whatever session is already active rather than re-applying config — so the Delta-configured session must be first, unconditionally. Docker (via `docker compose`) provides the JVM this needs without requiring one on the host; Tasks 3, 5 and (after Fix 1's relocation) 8's tests need no JVM and keep running on the host.

**Tech Stack:** Python, PySpark (local mode, `.enableHiveSupport()`), `delta-spark` (`configure_spark_with_delta_pip`), pytest, pandas, Docker (JVM-dependent tests only — see Task 7).

**Spec:** `docs/adr/0001-duckdb-backend-for-local-cdc-tests.md` (Stage 1 only — Stage 2/DuckDB is explicitly deferred and out of scope for this plan).

## Global Constraints

- `get_spark()` (`fabricks/utils/spark.py`) does change — Task 4 replaces its `DATABRICKS_LOCALMODE` boolean with a 3-way `FABRICKS_ENVIRONMENT` (`docker`/`remote`/`databricks`) and adds the `"docker"` branch Task 7's `local_spark` fixture delegates to; `build_spark_session()` (`fabricks/context/spark_session.py`) is untouched. The change is additive and backward-compatible: `FABRICKS_ENVIRONMENT` defaults to `"databricks"`, preserving every existing caller's behavior when nothing is set.
- `LocalFileSharePath` is a new sibling class under `BasePath`, not a fallback branch inside `FileSharePath`, and not a reuse of `GitPath`.
- Do not implement Stage 2 (DuckDB backend), the Parquet checkpoint/parallel-mode work, or CI wiring — this plan stops at one working sequential local suite covering job1's Gold-nocdc scenario and every contiguous job1-through-job9 Silver-king_and_queen-scd1/scd2 chain (see "Out of scope" for what's genuinely excluded — job10/11, and scenarios that skip job1 entirely).
- No `Bronze`/`Silver`/`Gold` job classes, `get_job()`, or job-config YAML files anywhere in this suite — every test constructs `NoCDC`/`SCD1`/`SCD2` directly, matching `tests/databricks/jobs/job1/test_cdc.py`'s established pattern. `Configurator.__init__` only needs `Database(database)` to resolve (a storage-path lookup via the `databases:` list in `conf.fabricks.yml`), not a job config.
- `tests/local/` is a new, separate pytest suite (not an extension of `tests/unit/`, which mocks `fabricks.context` wholesale and can't hold a real local Spark session in the same process) and not the Databricks-only `tests/databricks/`.
- The `tests/local/` suite must be run in its own separate `pytest` invocation (e.g. `pytest tests/local`), never combined with `tests/unit`/`tests/databricks` in the same process — `fabricks.context.runtime`'s module-level config resolution happens once per process and the first import wins.
- Reuse `tests/databricks/compare.py`'s `assert_dfs_equal` unchanged (import it directly — it doesn't touch the `SPARK` global). Do not reuse `compare_silver_to_expected`/`compare_gold_to_expected`/`create_expected_views` as-is (they hardcode `SPARK`/the Databricks-runtime `spark` global) — write local equivalents that take an explicit `spark` argument.
- Any `pytest tests/local/...` invocation — even one narrowly targeting a single file — loads `tests/local/conftest.py` first, unconditionally, because pytest's conftest loading is directory-scoped, not test-file-scoped. Since `conftest.py` builds a real Delta-configured `SparkSession` at module import time (see Task 7), this means **every** test under `tests/local/` requires a JVM/Docker once Task 7 lands, with no way to opt out per-file from inside that directory. This is why Task 8's fixture-generation test (pure pandas, no Spark) is placed under `tests/unit/` instead — see Task 8.
- Requires Docker (Docker Desktop or an equivalent daemon) to run any test under `tests/local/` — see Task 7 for the `Dockerfile`/`docker-compose.yml`. Tasks 5 (`tests/unit/test_local_file_share_path.py`) and 8 (relocated to `tests/unit/`) need no JVM and keep running as plain host `uv run pytest` commands. Tasks 1, 2 and 3 (the `tests/integration/` → `tests/databricks/` rename, the raw-fixture NDJSON conversion, and the `data/`/`expected/` extraction to shared top-level siblings) also need no JVM/Docker — all three are plain filesystem/format changes plus a find/replace.
- Task 1 (the rename) must land before any other task — every `tests/databricks/...` path referenced from Task 2 onward (fixture data, `expected/**/job*.sql`, `compare.py`) assumes the rename already happened. Task 2 (the raw-fixture NDJSON conversion) must land before Task 3, which moves those same raw files out of `tests/databricks/` — and before Task 8, which reads them from their post-move location — sequenced immediately after Task 1 since it depends on the rename's paths. Task 3 (extracting `data/`/`expected/` to shared top-level siblings) must land before Task 8 for the same reason, and after Task 2 so it moves files that are already NDJSON. Task 5 (`LocalFileSharePath`) has no dependency on any of the first three and could technically run first, but this plan sequences Tasks 1-3 first regardless, since they're the simplest tasks and de-risk every path/format reference that follows.

---

### Task 1: Rename `tests/integration/` to `tests/databricks/`

Naming-clarity request, not a design decision: "integration" doesn't say what the suite actually needs (a live Databricks cluster), where "databricks" does — matching the three-way split this plan already establishes by execution environment (`tests/unit/` no deps, `tests/local/` real local Spark+Delta, `tests/databricks/` Databricks cluster only). `tests/unit/` keeps its name. This is a plain rename plus find/replace — no logic changes anywhere — so its steps are mechanical, not TDD red/green cycles.

**Files:**
- Move (`git mv`, single directory move — preserves git history for all ~378 files under it in one operation, no need to move them individually): `framework/tests/integration/` → `framework/tests/databricks/`
- Modify (content only — path-string replace `tests/integration`→`tests/databricks`, `tests.integration`→`tests.databricks`, all occurrences in each file), internal to the moved directory (these files self-reference their own package/import path):
  - `framework/tests/databricks/armageddon.py`
  - `framework/tests/databricks/initialize.py`
  - `framework/tests/databricks/landing_to_raw.py`
  - `framework/tests/databricks/reset.py`
  - `framework/tests/databricks/run.py`
  - `framework/tests/databricks/runjobs.py`
  - `framework/tests/databricks/utils.py`
  - `framework/tests/databricks/jobs/job1/test_check.py`
  - `framework/tests/databricks/jobs/job1/test_gold.py`
  - `framework/tests/databricks/jobs/job1/test_invoke.py`
  - `framework/tests/databricks/jobs/job1/test_silver.py`
  - `framework/tests/databricks/jobs/job2/test_gold.py`
  - `framework/tests/databricks/jobs/job2/test_silver.py`
  - `framework/tests/databricks/jobs/job3/test_gold.py`
  - `framework/tests/databricks/jobs/job3/test_silver.py`
  - `framework/tests/databricks/jobs/job4/test_gold.py`
  - `framework/tests/databricks/jobs/job5/test_step.py`
- Modify (marker rename, `integration`→`databricks`, both the `pytest.mark.integration`/`pytest.mark.databricks` call and the `"integration" in str(item.fspath)`/`"databricks" in str(item.fspath)` fallback check): `framework/tests/databricks/conftest.py`
- Modify (external references — content path-string replace, all occurrences):
  - `framework/pyproject.toml` — the `integration` marker's description text (`[tool.pytest.ini_options]` `markers`) **and**, separately, the `[tool.fabricks]` section's `runtime`/`notebooks`/`config` default path values (these are load-bearing: they're what `ConfigOptions`/`fabricks.context.runtime` resolve against when no `FABRICKS_*` env vars override them, i.e. this repo's own default runtime pointer)
  - `framework/databricks.yml` — 3 occurrences (a commented-out `notebook_path` and two `destination:` bundle-deploy paths for `init.sh`)
  - `framework/README.md` — 4 doc references
  - `docs/TEST.md` — every reference to `tests/integration/` (the mechanical path swap only; the narrative "Local tests" section addition is Task 10, unaffected by this task)
  - `docs/ARCHITECTURE.md` — 2 references
- Do **not** touch `framework/.databricks/bundle/**` — it's gitignored (`framework/.gitignore:173`), regenerated by `databricks bundle deploy`, and will pick up the renamed paths automatically from the updated `databricks.yml` on the next deploy.

**Interfaces:**
- Produces: `framework/tests/databricks/` (renamed from `tests/integration/`) as the Databricks-only suite; `pytest.mark.databricks` (renamed from `pytest.mark.integration`) applied automatically by `tests/databricks/conftest.py` to every test under it. Every task from Task 6 onward references `tests/databricks/...` paths and assumes this rename already happened.

- [ ] **Step 1: Move the directory**

```bash
cd framework
git mv tests/integration tests/databricks
```

- [ ] **Step 2: Replace `tests/integration`/`tests.integration` in the files that self-reference their own path**

For each of the 17 files listed above under "internal to the moved directory," replace every occurrence of `tests/integration` with `tests/databricks` and `tests.integration` with `tests.databricks`. These are import statements (`from tests.integration.X import Y` → `from tests.databricks.X import Y`) and path-string literals; there is no logic change, only the string.

- [ ] **Step 3: Rename the pytest marker in `conftest.py`**

Modify `framework/tests/databricks/conftest.py`:

```python
"""Conftest for the Databricks-only integration tests - automatically applies databricks marker."""

from pathlib import Path

import pytest


def pytest_collection_modifyitems(items):
    """Automatically add 'databricks' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.databricks)
        except (ValueError, AttributeError):
            # Fallback for older Python or edge cases
            if "databricks" in str(item.fspath):
                item.add_marker(pytest.mark.databricks)
```

- [ ] **Step 4: Update `pyproject.toml`**

Modify `framework/pyproject.toml`'s `[tool.pytest.ini_options]` `markers` list — rename the marker and its description:

```toml
markers = [
    "unit: marks tests as unit tests (no spark dependencies)",
    "databricks: marks tests as Databricks-cluster integration tests (slower, needs a live cluster)",
    "local: marks tests as local CDC/DDL tests (real local Spark+Delta, no Databricks)",
]
```

Separately, in the same file's `[tool.fabricks]` section, replace `tests/integration/runtime` → `tests/databricks/runtime`, `tests/integration/runtime/fabricks/notebooks` → `tests/databricks/runtime/fabricks/notebooks`, and `tests/integration/runtime/fabricks/conf.uc.fabricks.yml` → `tests/databricks/runtime/fabricks/conf.uc.fabricks.yml` (the `runtime`/`notebooks`/`config` keys — this is this repo's own default `FABRICKS_RUNTIME`/`FABRICKS_NOTEBOOKS`/`FABRICKS_CONFIG` fallback when no environment variable overrides it, so it must point at the renamed directory or every command that relies on the default silently breaks).

- [ ] **Step 5: Update `databricks.yml`, `README.md`, `TEST.md`, `ARCHITECTURE.md`**

Replace all `tests/integration` occurrences with `tests/databricks` in `framework/databricks.yml` (3 occurrences), `framework/README.md` (4 occurrences), `docs/TEST.md` (all occurrences — mechanical path swap only, do not otherwise restructure this file here, that's Task 10), and `docs/ARCHITECTURE.md` (2 occurrences).

- [ ] **Step 6: Verify no stale references remain**

Run (from the repo root):

```bash
grep -rn "tests/integration\|tests\.integration" --include="*.py" --include="*.md" --include="*.toml" --include="*.yml" --include="*.yaml" --include="*.json" --include="*.sh" . | grep -v "^\./framework/\.databricks/"
```

Expected: no output. If anything remains, it's either a file this step's inventory missed (fix it) or a legitimate historical reference (e.g. a changelog entry describing a past state) — if the latter, confirm it's genuinely historical before leaving it, don't assume.

- [ ] **Step 7: Confirm the renamed suite still collects and the unrelated suite is unaffected**

This task has no failing/passing unit test of its own (it's a pure rename) — the verification is that pytest can still discover and construct every test after the move, and that `tests/unit/` (untouched) still passes:

Run: `cd framework && uv run pytest --collect-only tests/databricks`
Expected: the same tests collect as before the rename (same count), no import errors.

Run: `cd framework && uv run pytest tests/unit -v`
Expected: all PASS, unchanged — confirms this task didn't disturb the unrelated suite.

(`tests/databricks/`'s tests themselves still require a live Databricks cluster to actually *run*, per `docs/TEST.md` — `--collect-only` is what's checkable here, same limitation this suite already had before the rename.)

- [ ] **Step 8: Commit**

Two commits — the `git mv` first (keeps git's rename detection clean, since a mv+edit in one commit can make git treat it as a delete+add instead of a rename in some diff views), then the content edits:

```bash
git add -A framework/tests/databricks
git commit -m "chore: rename tests/integration to tests/databricks for clarity

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"

git add framework/tests/databricks framework/pyproject.toml framework/databricks.yml framework/README.md docs/TEST.md docs/ARCHITECTURE.md
git commit -m "chore: update tests/integration references to tests/databricks

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: Convert raw landing fixtures to NDJSON

Format-consistency request, not a design decision: the raw landing fixtures under `tests/databricks/data/job*/**/*.json` are currently JSON arrays (`[{...}, {...}, {...}]`); Task 8's derived Stage-1 fixtures are NDJSON (one JSON object per line, chosen for git-diffability and native struct support — see ADR 0001). Converting the raw files to NDJSON too makes the whole fixture pipeline (raw input and derived output) consistently one format. Same records, same values — a reformat, not a content change. Like Task 1, this is mechanical (no logic change to what the data *means*), so its steps are a conversion pass plus verification, not TDD red/green.

This task must land before Task 8, which reads these same raw files — Task 8's `derive_rows`/its test are updated here to match.

**Files:**
- Modify (content reformat, JSON array → NDJSON, one line per record, same field values) — all `*.json` files under `framework/tests/databricks/data/job*/**` (~45 files, glob-driven, not individually enumerated below).
- Modify: `framework/tests/databricks/utils.py` — `convert_json_to_parquet`'s `pd.read_json(f, orient="records", convert_dates=dates)` call needs `lines=True` added to keep reading these files correctly now that they're NDJSON, not a JSON array.
- Modify (once Task 8 exists later in this plan): `framework/tests/local/generate_fixtures.py`'s `derive_rows` (`pd.read_json(json_file, orient="records", convert_dates=_DATE_COLUMNS)`) needs the same `lines=True` addition — see Task 8, which is written directly against this task's output and Task 3's post-move `tests/data/` location, not against a stale path needing correction.

**Interfaces:**
- Produces: every raw fixture file under `tests/databricks/data/` in NDJSON format instead of JSON-array format. `convert_json_to_parquet` (existing Databricks-suite code) and `derive_rows` (Task 8) both updated to read it correctly.

- [ ] **Step 1: Convert the raw fixture files**

```python
# one-off conversion script — not committed, run once and discarded
import json
from pathlib import Path

DATA_ROOT = Path("framework/tests/databricks/data")

for json_file in sorted(DATA_ROOT.rglob("*.json")):
    records = json.loads(json_file.read_text())
    assert isinstance(records, list), f"{json_file} is not a JSON array — check before converting"
    lines = [json.dumps(record) for record in records]
    json_file.write_text("\n".join(lines) + "\n")
```

Run this from the repo root (`python <script>`, no dependencies beyond stdlib `json`). It rewrites each file in place — same records, same field values, one line per record instead of one array.

- [ ] **Step 2: Verify a converted file's shape**

```bash
head -1 framework/tests/databricks/data/job1/king/2022/01/01/0001/king_202201010001.json
```

Expected: a single JSON object on the first line (e.g. `{"id": 1, "name": "Leopold I", ...}`), not an opening `[`. Spot-check 2-3 more files across different job folders (a `__deletelog` file and a later `jobN` folder) to confirm the conversion applied consistently, not just to the first file touched.

- [ ] **Step 3: Update `convert_json_to_parquet` to read NDJSON**

Modify `framework/tests/databricks/utils.py`'s `convert_json_to_parquet` — add `lines=True` to its `pd.read_json` call:

```python
p_df = pd.read_json(f, orient="records", lines=True, convert_dates=cast(Any, dates))
```

(Only this one call site changes — nothing else in `convert_json_to_parquet` depends on the JSON-array vs. NDJSON distinction.)

- [ ] **Step 4: Verify `convert_json_to_parquet`'s change against a real converted file**

This function is part of the existing, currently-working Databricks suite — it also calls `spark.createDataFrame(p_df)` and needs a live `spark`/`dbutils` from `databricks.sdk.runtime`, which this local dev environment doesn't have, so a full end-to-end run of `convert_json_to_parquet` itself isn't checkable here. What *is* checkable without Databricks is that `lines=True` correctly parses one of the now-NDJSON files on its own:

```bash
cd framework && uv run python -c "
import pandas as pd
df = pd.read_json('tests/databricks/data/job1/king/2022/01/01/0001/king_202201010001.json', orient='records', lines=True)
print(len(df), 'rows')
print(df.columns.tolist())
"
```

Expected: prints the same row count and column names this file had before the conversion (3 rows, `id`/`name`/`doubleField`/`BEL_DeleteDateUtc`/`BEL_RestoredDateUtc`/`BEL_IsFullLoad`/`BEL_UpdateDateUtc`, per the file's known content). This confirms the *pandas read* half of the change is correct; full confidence that `convert_json_to_parquet` still works end-to-end only comes from the next real Databricks CI/cluster run — out of scope for this plan to trigger, and worth calling out explicitly rather than silently assuming it's fine.

- [ ] **Step 5: Verify no raw `.json` file is still a JSON array**

```bash
cd framework && uv run python -c "
import json
from pathlib import Path

bad = []
for f in sorted(Path('tests/databricks/data').rglob('*.json')):
    first_char = f.read_text().lstrip()[0]
    if first_char == '[':
        bad.append(f)

assert not bad, f'still JSON-array format: {bad}'
print('all NDJSON')
"
```

Expected: prints `all NDJSON`, no assertion error.

- [ ] **Step 6: Commit**

```bash
git add framework/tests/databricks/data framework/tests/databricks/utils.py
git commit -m "chore: convert raw landing fixtures from JSON arrays to NDJSON

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: Extract `data/`/`expected/` to shared locations; convert `scd2` root to NDJSON

Coupling-removal request, not a design decision: `tests/databricks/data/` and `tests/databricks/expected/` hold fixture input and golden comparison output that both `tests/databricks/` (existing suite) and `tests/local/` (this plan, from Task 8 onward) consume — leaving them nested under `tests/databricks/` means `tests/local/` has to reach into a directory named for a different suite to get its own fixtures. Moving them to `framework/tests/data/` and `framework/tests/expected/` (top-level siblings of `tests/unit/`, `tests/local/`, `tests/databricks/`) removes that reach-through. Same shape as Task 1: a plain move plus find/replace, no logic change, so its steps are mechanical, not TDD red/green.

Sequenced here — after Task 2 (so it moves files that are already NDJSON) and before Task 8 (the fixture-derivation script, which reads from the new location).

**Files:**
- Move (`git mv`): `framework/tests/databricks/data/` → `framework/tests/data/`
- Move (`git mv`): `framework/tests/databricks/expected/` → `framework/tests/expected/`
- Convert: `framework/tests/expected/silver/scd2/job01.sql` → `framework/tests/expected/silver/scd2/job01.ndjson` (see Step 2b below — the `silver/scd2` layer is the root of the `expected/**` DAG per job: a literal `create ... from values (...)` list, i.e. genuinely hardcoded data, not a query. Every other file under `expected/**` — `gold/scd2`, `gold/scd1`, `gold/scd0`, `silver/scd1`, `silver/scd0` — is a derived SQL view referencing `expected.silver_scd2_job{N}` by name (e.g. `silver/scd1/jobNN.sql`: `select * except(__valid_from,__valid_to) from expected.silver_scd2_job{N} qualify row_number()... = 1`; `gold/scd2/jobNN.sql`: `select ... from expected.silver_scd2_job{N}`) and stays exactly as SQL, unmodified — none of them care how the root view was populated, only that `expected.silver_scd2_job{N}` exists and is queryable. Converting only the root avoids keeping two sources of truth for the same 5 rows (a `.sql` and a `.ndjson` side by side would drift against each other) while keeping every real derived-query relationship intact.)
- Modify: `framework/tests/databricks/utils.py` — `git_to_landing()`'s `from_dir = paths.tests.joinpath("data", job)` and `create_expected_views()`'s `views = paths.tests.joinpath("expected", step, cdc)` both resolve relative to `paths.tests` (`tests/databricks/`, per `_types.py`'s `Paths.tests = GitPath(PATH_RUNTIME.pathlibpath.parent.resolve())`) — since `data`/`expected` are no longer under that directory, both need `paths.tests.parent.joinpath(...)` instead (`paths.tests.parent` = `tests/`, the new shared parent of both). `create_expected_views()`'s `_create_views` also needs to become NDJSON-aware for the `silver`/`scd2` case (Step 2b) — extending it here, rather than leaving it reading a now-deleted `.sql` file, keeps a single source of truth for the root data shared by both `tests/databricks/` and `tests/local/` (Task 9), rather than this plan silently breaking the existing Databricks suite's own `create_expected_views()`.
- Modify: `docs/TEST.md` — already references `tests/expected/...`, updated ahead of this task since it doesn't depend on Task 1/Task 2 having run first; no further change needed here.
- Modify: `docs/adr/0001-duckdb-backend-for-local-cdc-tests.md` — already updated (ahead of this task) to reference `tests/expected/**` and `tests/data/**`; no further change needed here.

**Interfaces:**
- Produces: `framework/tests/data/` and `framework/tests/expected/` as shared, suite-neutral fixture locations. Every task from Task 8 onward references `tests/data/...`/`tests/expected/...` and assumes this move already happened.

- [ ] **Step 1: Move the directories**

```bash
cd framework
git mv tests/databricks/data tests/data
git mv tests/databricks/expected tests/expected
```

- [ ] **Step 2: Fix `tests/databricks/utils.py`'s path construction**

Modify `framework/tests/databricks/utils.py`:

```python
def git_to_landing():
    DEFAULT_LOGGER.info("git to landing")
    for i in range(1, 12):
        job = f"job{i}"
        DEFAULT_LOGGER.debug(f"copy json from git to landing ({job})")

        from_dir = paths.tests.parent.joinpath("data", job)
        to_dir = paths.landing.joinpath(job)

        convert_json_to_parquet(from_dir, to_dir)
```

```python
import json
import re

from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

_EXPECTED_SCD2_SCHEMA = StructType(
    [
        StructField("__valid_from", TimestampType(), True),
        StructField("__valid_to", TimestampType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)


def create_expected_views():
    DEFAULT_LOGGER.info("expected - create views")

    def _create_views(step: str, cdc: str):
        views = paths.tests.parent.joinpath("expected", step, cdc)

        if step == "silver" and cdc == "scd2":
            # Only job1's file is hand-authored NDJSON data (see Task 3's
            # Files note) — job2.sql onward are still real SQL, each unioning
            # its OWN new VALUES rows with `select ... from
            # expected.silver_scd2_job{N-1} where not __is_current` (verified:
            # job3.sql references job2, job2.sql references job1 — a genuine
            # sequential chain, not all pointing at job1). So this branch
            # must create job1's NDJSON root *then fall through* to the SQL
            # loop below for job2 onward, in ascending job-number order — an
            # early `return` here would silently skip creating
            # expected.silver_scd2_job{2..9} entirely, since nothing else in
            # this function ever visits this directory's .sql files.
            for v in sorted(views.walk(file_format="ndjson")):
                DEFAULT_LOGGER.debug(f"create table {v}")
                job_num = re.search(r"\d+", GitPath(v).get_file_name()).group()
                rows = [json.loads(line) for line in GitPath(v).pathlibpath.read_text().splitlines()]
                df = spark.createDataFrame(rows, schema=_EXPECTED_SCD2_SCHEMA)
                df.write.mode("overwrite").saveAsTable(f"expected.silver_scd2_job{job_num}")

        for v in sorted(views.walk(file_format="sql")):
            DEFAULT_LOGGER.debug(f"create view {v}")
            spark.sql(GitPath(v).get_sql())

    _create_views("silver", "scd2")
    _create_views("silver", "scd1")

    _create_views("gold", "scd2")
    _create_views("gold", "scd1")
    _create_views("gold", "scd0")
```

(`from_dir`/`views` gain `.parent` before `.joinpath(...)`, same as before. `_create_views` also gains: (a) `views.walk(file_format=...)` instead of the previous unfiltered `views.walk()` — `GitPath.walk` already supports this parameter; passing it explicitly is what makes it safe for a directory to hold more than one file extension, which `silver/scd2/` now does across the two suites' expectations even though `tests/databricks/`'s own walk only ever touches `.ndjson` there post-conversion — the `.sql` branch's explicit `file_format="sql"` is defensive, not because `.sql` and `.ndjson` files are ever mixed in the same call; (b) the `silver`/`scd2` special case, loading NDJSON with an explicit schema and registering it as a real table (`saveAsTable`, not a temp view — temp views aren't database-qualified, and `gold/scd2/jobNN.sql`'s `from expected.silver_scd2_job{N}` needs a genuine catalog-addressable name in the `expected` database) instead of running SQL text.)

- [ ] **Step 2b: Convert `silver/scd2/job01.sql` to `job01.ndjson`**

Read the current file first (`framework/tests/expected/silver/scd2/job01.sql`, already moved here by Step 1) to confirm its exact 5 rows before converting — do not transcribe from memory. Convert its `values (...)` rows (in the same `id, __valid_from` order the original file's trailing `order by` produces) to one JSON object per line:

```json
{"__valid_from": "1900-01-01 00:00:00", "__valid_to": "9999-12-31 00:00:00", "id": 1, "name": "Leopold I", "doubleField": 0.19000101, "__is_current": true, "__is_deleted": false, "__source": "king"}
{"__valid_from": "1900-01-01 00:00:00", "__valid_to": "2022-01-02 00:00:59", "id": 2, "name": "Leopold II", "doubleField": 0.19000101, "__is_current": false, "__is_deleted": false, "__source": "king"}
{"__valid_from": "2022-01-02 00:01:00", "__valid_to": "9999-12-31 00:00:00", "id": 2, "name": "Leopold II", "doubleField": 0.20220102, "__is_current": true, "__is_deleted": false, "__source": "king"}
{"__valid_from": "1900-01-01 00:00:00", "__valid_to": "2022-01-04 00:00:59", "id": 101, "name": "Louise", "doubleField": 0.19000101, "__is_current": false, "__is_deleted": true, "__source": "queen"}
{"__valid_from": "2022-01-02 00:01:00", "__valid_to": "2022-01-03 00:00:59", "id": 201, "name": "Marie-Henriette", "doubleField": 0.20220102, "__is_current": false, "__is_deleted": true, "__source": "queen"}
```

Write this to `framework/tests/expected/silver/scd2/job01.ndjson`, then `git rm framework/tests/expected/silver/scd2/job01.sql` — the NDJSON file is now the single source of truth for job1's SCD2 root data; `silver/scd1/job01.sql`, `gold/scd2/job01.sql`, `gold/scd1/job01.sql`, `gold/scd0/job01.sql` are untouched and keep deriving from `expected.silver_scd2_job1` by name, same as before. Only job1's file is converted in this plan — `job02.sql` through `job11.sql` at the `silver/scd2` layer stay as `.sql`, untouched, and (unlike job1's) are *not* candidates for NDJSON conversion at all: read directly, `job02.sql`/`job03.sql` are each `VALUES (...) union all select ... from expected.silver_scd2_job{N-1} where not __is_current` — a genuine sequential chain of hand-authored rows referencing the *previous* job's view, not a standalone data root the way job1's was. `_create_views`/`create_expected_views` (Step 2 above, and Task 9's local port) both need to run these `.sql` files too, in ascending order, for job2 through job9's scenarios (Task 8/9) to have anything to compare against — see the "Bug fix" note in the Self-Review for a real early-`return` bug this surfaced and fixed.

- [ ] **Step 3: Verify no stale references remain**

```bash
cd .. && grep -rn "tests/databricks/data\|tests\.databricks\.data\|tests/databricks/expected\|tests\.databricks\.expected" --include="*.py" --include="*.md" --include="*.toml" --include="*.yml" --include="*.yaml" --include="*.json" --include="*.sh" . | grep -v "^\./framework/\.databricks/"
```

Expected: no output.

- [ ] **Step 4: Confirm the moved directories are where expected and `tests/databricks` still collects**

```bash
ls framework/tests/data/job1 framework/tests/expected/silver
ls framework/tests/expected/silver/scd2/job01.ndjson
```

Expected: both `ls` calls list files (job1's fixture folders; `scd0`/`scd1`/`scd2` subdirectories respectively) — confirms the move landed and nothing was left empty. The second `ls` confirms Step 2b's conversion produced the expected file at the expected path (and, implicitly, that `job01.sql` no longer exists there).

This task has no failing/passing test of its own (a pure move, like Task 1) — `tests/databricks/`'s suite still needs a live Databricks cluster to actually run its tests against the moved paths, per `docs/TEST.md`'s existing limitation, unchanged by this move. Full confidence that `git_to_landing`/`create_expected_views` still work end-to-end only comes from the next real Databricks CI/cluster run — same caveat Task 2 already states for `convert_json_to_parquet`.

Run (still checkable without Databricks): `cd framework && uv run pytest --collect-only tests/databricks` and `cd framework && uv run pytest tests/unit -v` — same collection/unrelated-suite checks Task 1 used, confirming this move didn't break test discovery or disturb `tests/unit/`.

- [ ] **Step 5: Commit**

```bash
git add framework/tests/data framework/tests/expected framework/tests/databricks/utils.py
git commit -m "chore: extract tests/databricks/{data,expected} to shared tests/{data,expected}, convert silver/scd2/job01 root to NDJSON

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 4: Replace `DATABRICKS_LOCALMODE` with `FABRICKS_ENVIRONMENT`

Environment-modeling request, not purely mechanical: `framework/fabricks/utils/spark.py` currently has `DATABRICKS_LOCALMODE: Final[bool] = os.getenv("DATABRICKS_LOCALMODE", "false")...`, gating `get_spark()`/`get_dbutils()`/`display()`. `=true` means "local machine using Databricks Connect to reach a *remote* cluster" (`DatabricksSession` via `DATABRICKS_CLUSTER_ID`/`DATABRICKS_PROFILE`); `=false` (the default) falls through to plain `SparkSession.builder.getOrCreate()` — which today conflates two genuinely different environments: running natively inside a real Databricks cluster/notebook, and this plan's new fully-local Docker+Delta test environment (neither has a live Databricks connection, but only one is Stage 1). A boolean can't express three states. Replace it with a proper 3-value environment: `docker` (this plan's local Docker+Spark+Delta suite — no Databricks, no dbutils, no cloud storage), `remote` (today's `=true` case, unchanged), `databricks` (today's `=false` case, unchanged, and the default — preserves existing behavior for every deployment that sets nothing).

Blast radius is small and already verified: only 3 files in the whole repo reference `DATABRICKS_LOCALMODE` — its definition/usage in `utils/spark.py`, and one mock attribute (`DATABRICKS_LOCALMODE=False`) in `framework/tests/unit/conftest.py`'s Spark-module mock. No deployment scripts, CI config, or `databricks.yml` reference it — safe to replace cleanly.

`FABRICKS_ENVIRONMENT` lives in a **new**, deliberately Spark-free module (`framework/fabricks/utils/environment.py`), not directly in `utils/spark.py` — because Task 5 (`resolve_fileshare_path`'s dispatch) also needs this value, and `fabricks/utils/path/__init__.py`'s own docstring is "Path utilities without Spark dependencies": `utils/spark.py` imports `pyspark`/`databricks.sdk` at module level and eagerly builds a `SparkSession` on import (`spark = get_spark()` at its own bottom) — importing it from `utils/path/file_share.py` would both violate that Spark-free contract and trigger an eager session build as a side effect of resolving a path. A small shared module with just the env-var read avoids both.

**Files:**
- Create: `framework/fabricks/utils/environment.py`
- Modify: `framework/fabricks/utils/spark.py` — remove `DATABRICKS_LOCALMODE`, rewrite `get_spark()`/`get_dbutils()`/`display()` to branch on `FABRICKS_ENVIRONMENT`
- Modify: `framework/tests/unit/conftest.py` — its `DATABRICKS_LOCALMODE=False` mock attribute is stale once the real module no longer has that name; delete the line (nothing reads it off the mock today — confirmed by the 3-file search above — so this is a cleanliness fix, not a behavior fix)
- Test: `framework/tests/unit/test_environment.py`

**Interfaces:**
- Produces: `FABRICKS_ENVIRONMENT: Final[Literal["docker", "remote", "databricks"]]` in `fabricks.utils.environment`, read once from `os.getenv("FABRICKS_ENVIRONMENT", "databricks")` at module import time (same eager-singleton pattern already used throughout this codebase — `CONF_RUNTIME`, `SPARK`, the old `DATABRICKS_LOCALMODE` itself). Consumed by `utils/spark.py` (this task) and Task 5's `resolve_fileshare_path` dispatch.
- Produces: `get_spark()`/`get_dbutils()`/`display()` (`fabricks.utils.spark`) now branch on `FABRICKS_ENVIRONMENT` instead of the removed `DATABRICKS_LOCALMODE` boolean. `get_spark()`'s `"docker"` branch is new: builds the same Delta-configured session Task 7's `local_spark` fixture needs (`configure_spark_with_delta_pip`, Delta extensions/catalog config, Hive support), plus enables `spark.databricks.delta.schema.autoMerge.enabled` (needed for Task 9's job1→job9 chains, which merge job2+'s new `newField` column into a job1-created table schema — see the code block's comment for why, and why `resolveMergeUpdateStructsByName` isn't also added) — Task 7 delegates to this instead of duplicating the builder inline (see Task 7).

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/unit/test_environment.py
import importlib

import pytest


def test_default_is_databricks(monkeypatch):
    monkeypatch.delenv("FABRICKS_ENVIRONMENT", raising=False)
    from fabricks.utils import environment

    importlib.reload(environment)
    assert environment.FABRICKS_ENVIRONMENT == "databricks"


def test_docker_value(monkeypatch):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "docker")
    from fabricks.utils import environment

    importlib.reload(environment)
    assert environment.FABRICKS_ENVIRONMENT == "docker"


def test_invalid_value_raises(monkeypatch):
    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "bogus")
    from fabricks.utils import environment

    with pytest.raises(AssertionError):
        importlib.reload(environment)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd framework && uv run pytest tests/unit/test_environment.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'fabricks.utils.environment'`

- [ ] **Step 3: Write `fabricks/utils/environment.py`**

```python
# framework/fabricks/utils/environment.py
import os
from typing import Final, Literal

FabricksEnvironment = Literal["docker", "remote", "databricks"]

_ENVIRONMENTS: tuple[FabricksEnvironment, ...] = ("docker", "remote", "databricks")
_raw = os.getenv("FABRICKS_ENVIRONMENT", "databricks").lower()
assert _raw in _ENVIRONMENTS, f"FABRICKS_ENVIRONMENT must be one of {_ENVIRONMENTS}, got {_raw!r}"
FABRICKS_ENVIRONMENT: Final[FabricksEnvironment] = _raw  # type: ignore[assignment]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd framework && uv run pytest tests/unit/test_environment.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Rewrite `fabricks/utils/spark.py`**

```python
# framework/fabricks/utils/spark.py
import os

from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import DataFrame, SparkSession

from fabricks.utils.environment import FABRICKS_ENVIRONMENT


def get_spark() -> SparkSession:
    if FABRICKS_ENVIRONMENT == "remote":
        from databricks.connect.session import DatabricksSession
        from databricks.sdk.core import Config

        profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        assert cluster_id, "DATABRICKS_CLUSTER_ID environment variable is not set"

        c = Config(profile=profile, cluster_id=cluster_id)

        spark = DatabricksSession.builder.sdkConfig(c).getOrCreate()

    elif FABRICKS_ENVIRONMENT == "docker":
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("fabricks-docker")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.allowMultipleContexts", "true")
            .enableHiveSupport()
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        # Job-sequential Silver scenarios (Task 9) merge job2+'s data — which
        # introduces columns job1's schema doesn't have (verified: job2 adds
        # `newField`, still present through job9) — into a table whose schema
        # was created from an earlier job. This plan bypasses job
        # orchestration entirely (no Bronze/Silver/Gold classes, no
        # update_schema() between jobs), so without autoMerge a `MERGE INTO`
        # referencing a new source column would fail with a real schema
        # mismatch. Matches production's own fix for this (`add_spark_options_to_spark()`
        # in fabricks/context/spark_session.py already sets this for every
        # real Databricks session) rather than inventing local-only schema-
        # reconciliation logic. Confirmed via Delta Lake's own OSS docs this
        # is a core open-source feature (available since Delta 0.6.0), not
        # Databricks-Runtime-only, despite the `spark.databricks.*` config
        # namespace. Does not need resolveMergeUpdateStructsByName alongside
        # it: this plan's fixture data has no `__metadata`/struct columns
        # (verified — `has_metadata = "__metadata" in columns`,
        # `fabricks/cdc/base/processor.py`), so the struct-field merge clause
        # that setting affects is never emitted here; add it only if a future
        # job's data actually introduces a struct column.
        spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = true")

    else:
        spark = SparkSession.builder.getOrCreate()

    assert spark is not None
    return spark


def display(df: DataFrame, limit: int | None = None) -> None:
    """
    Display a Spark DataFrame. Uses IPython/pandas display outside a native
    Databricks runtime (FABRICKS_ENVIRONMENT != "databricks"); the
    Databricks-injected display otherwise.
    """
    if FABRICKS_ENVIRONMENT != "databricks":
        from IPython.display import display

        if limit is not None:
            df = df.limit(limit)

        display(df.toPandas())

    else:
        from databricks.sdk.runtime import display

        if limit is not None:
            df = df.limit(limit)

        display(df)


def get_dbutils(spark: SparkSession | None = None) -> RemoteDbUtils | None:
    try:
        if FABRICKS_ENVIRONMENT == "remote":
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            dbutils = w.dbutils

        else:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(spark)

        assert dbutils is not None
        return dbutils  # type: ignore

    except Exception:
        return None


spark = get_spark()
dbutils = get_dbutils(spark=spark)
```

`get_dbutils()`'s `"docker"` case falls into the `else` branch (`DBUtils(spark)`) alongside `"databricks"`, not a new dedicated branch: it has no real dbutils backend either way in a fully-local test environment, and the surrounding `try/except Exception: return None` already degrades it to `None` gracefully — same outcome as adding a no-op branch, less code. `display()` groups `"docker"` with `"remote"` (both non-native-Databricks environments use the IPython path) rather than with `"databricks"` — this is the one place the old boolean's grouping doesn't carry over 1:1 (`DATABRICKS_LOCALMODE=false` used to mean *only* "databricks"; now `!= "databricks"` correctly covers both non-native cases). `display()` isn't called anywhere in this plan's tests, so this is unverified by this plan's own test suite — flagged, not a blocker.

- [ ] **Step 6: Update `tests/unit/conftest.py`'s mock**

Modify `framework/tests/unit/conftest.py` — remove the `DATABRICKS_LOCALMODE=False` line from the `MagicMock(...)` call at `sys.modules["fabricks.utils.spark"] = MagicMock(...)` (the mock replaces the whole module, so this is a cleanliness fix matching the real module's new shape, not a behavior change — nothing in the test suite currently reads this attribute off the mock).

- [ ] **Step 7: Run the full unit suite to confirm nothing broke**

Run: `cd framework && uv run pytest tests/unit -v`
Expected: all PASS

- [ ] **Step 8: Commit**

```bash
git add framework/fabricks/utils/environment.py framework/fabricks/utils/spark.py framework/tests/unit/conftest.py framework/tests/unit/test_environment.py
git commit -m "feat: replace DATABRICKS_LOCALMODE with a 3-way FABRICKS_ENVIRONMENT (docker/remote/databricks)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 5: `LocalFileSharePath` and environment-based dispatch

**Files:**
- Create: `framework/fabricks/utils/path/local.py`
- Modify: `framework/fabricks/utils/path/__init__.py:1-11`
- Modify: `framework/fabricks/utils/path/file_share.py:144-176` (`resolve_fileshare_path`)
- Test: `framework/tests/unit/test_local_file_share_path.py`

**Interfaces:**
- Consumes: `FABRICKS_ENVIRONMENT` from `fabricks.utils.environment` (Task 4).
- Produces: `LocalFileSharePath(BasePath)` in `fabricks.utils.path.local`, re-exported from `fabricks.utils.path`. Constructor: `LocalFileSharePath(path: str | PathlibPath)`. Methods: `exists() -> bool`, `walk(depth=None, convert=False, file_format=None) -> list`, `_yield(path) -> Iterator[str]`, `rm() -> None`. Same signatures as `FileSharePath`/`GitPath`'s equivalents (`BasePath`'s abstract contract).
- Produces: `resolve_fileshare_path(...)` (same signature, `utils/path/file_share.py`) now returns a `LocalFileSharePath` when `FABRICKS_ENVIRONMENT == "docker"`, a `FileSharePath` otherwise — replacing the URI-scheme sniff (`abfss://` vs not) with an explicit environment check. The `abfss://` prefix check isn't dropped, though: it's kept as an assertion for the non-`"docker"` branch, catching a real misconfiguration class the old shape-based dispatch would have silently mishandled — a storage path that doesn't look like `abfss://` while `FABRICKS_ENVIRONMENT` is `"remote"`/`"databricks"` now fails loudly instead of quietly becoming a `LocalFileSharePath` by accident of string shape.

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/unit/test_local_file_share_path.py
from pathlib import Path

from fabricks.utils.path.local import LocalFileSharePath


def test_exists_false_for_missing_path(tmp_path: Path):
    p = LocalFileSharePath(str(tmp_path / "missing"))
    assert p.exists() is False


def test_exists_true_and_walk_lists_files(tmp_path: Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("b")

    p = LocalFileSharePath(str(tmp_path))
    assert p.exists() is True

    found = {Path(f).name for f in p.walk()}
    assert found == {"a.txt", "b.txt"}


def test_walk_file_format_filter(tmp_path: Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.parquet").write_text("b")

    p = LocalFileSharePath(str(tmp_path))
    found = p.walk(file_format="parquet")
    assert len(found) == 1
    assert found[0].endswith("b.parquet")


def test_rm_removes_directory(tmp_path: Path):
    target = tmp_path / "victim"
    target.mkdir()
    (target / "f.txt").write_text("x")

    p = LocalFileSharePath(str(target))
    assert p.exists() is True
    p.rm()
    assert p.exists() is False


def test_joinpath_preserves_class(tmp_path: Path):
    p = LocalFileSharePath(str(tmp_path))
    child = p.joinpath("sub", "dir")
    assert isinstance(child, LocalFileSharePath)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd framework && uv run pytest tests/unit/test_local_file_share_path.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'fabricks.utils.path.local'`

- [ ] **Step 3: Write `LocalFileSharePath`**

```python
# framework/fabricks/utils/path/local.py
from collections.abc import Iterator
from pathlib import Path as PathlibPath
import shutil

from fabricks.utils.path.base import BasePath


class LocalFileSharePath(BasePath):
    """A BasePath backed by the plain local filesystem (pathlib), no dbutils.

    Used as the storage root for local (non-Databricks) tests, where
    FileSharePath's dbutils.fs.* calls have no equivalent.
    """

    def __init__(self, path: str | PathlibPath) -> None:
        super().__init__(path=path)

    def exists(self) -> bool:
        return self.pathlibpath.exists()

    def walk(self, depth: int | None = None, convert: bool | None = False, file_format: str | None = None) -> list:  # noqa: ARG002 - `depth` kept to match BasePath.walk
        if not self.exists():
            return []

        if self.pathlibpath.is_file():
            out = [self.string]
        else:
            out = list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(file_format)]

        if convert:
            out = [self.__class__(o) for o in out]

        return out

    def _yield(self, path: str | PathlibPath) -> Iterator[str]:
        if isinstance(path, str):
            path = PathlibPath(path)

        for child in path.glob("*"):
            if child.is_dir():
                yield from self._yield(child)
            else:
                yield str(child)

    def rm(self) -> None:
        if self.exists():
            if self.pathlibpath.is_dir():
                shutil.rmtree(self.pathlibpath)
            else:
                self.pathlibpath.unlink()
```

- [ ] **Step 4: Export it from `fabricks.utils.path`**

Modify `framework/fabricks/utils/path/__init__.py`:

```python
"""Path utilities without Spark dependencies."""

from pathlib import Path as PathlibPath

from typing_extensions import deprecated

from fabricks.utils.path.base import BasePath
from fabricks.utils.path.file_share import FileSharePath, resolve_fileshare_path
from fabricks.utils.path.git import GitPath, resolve_git_path
from fabricks.utils.path.local import LocalFileSharePath

__all__ = [
    "BasePath",
    "FileSharePath",
    "GitPath",
    "LocalFileSharePath",
    "Path",
    "resolve_fileshare_path",
    "resolve_git_path",
]
```

(Only the `from fabricks.utils.path.local import LocalFileSharePath` line and the `__all__` list change; the rest of the file — the deprecated `Path` class — is unchanged.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd framework && uv run pytest tests/unit/test_local_file_share_path.py -v`
Expected: PASS (5 passed)

- [ ] **Step 6: Write the failing test for `resolve_fileshare_path` dispatch**

`FABRICKS_ENVIRONMENT` (Task 4) is read once at `fabricks.utils.environment`'s module import time — testing different values means `monkeypatch.setenv` followed by `importlib.reload(...)` on both that module and `fabricks.utils.path.file_share` (which imports `FABRICKS_ENVIRONMENT` from it), same pattern Task 4's own tests use. A small fixture avoids repeating this three times.

```python
# append to framework/tests/unit/test_local_file_share_path.py
import importlib

import pytest

from fabricks.utils.path import FileSharePath, resolve_fileshare_path
from fabricks.utils.path.local import LocalFileSharePath


@pytest.fixture
def set_environment(monkeypatch):
    from fabricks.utils import environment as environment_module
    from fabricks.utils.path import file_share as file_share_module

    def _set(value: str):
        monkeypatch.setenv("FABRICKS_ENVIRONMENT", value)
        importlib.reload(environment_module)
        importlib.reload(file_share_module)
        return file_share_module

    yield _set

    monkeypatch.delenv("FABRICKS_ENVIRONMENT", raising=False)
    importlib.reload(environment_module)
    importlib.reload(file_share_module)


def test_resolve_fileshare_path_docker_returns_local(tmp_path, set_environment):
    fs = set_environment("docker")
    p = fs.resolve_fileshare_path(str(tmp_path / "gold"))
    assert isinstance(p, LocalFileSharePath)


def test_resolve_fileshare_path_databricks_returns_fileshare(set_environment):
    fs = set_environment("databricks")
    p = fs.resolve_fileshare_path("abfss://gold@storage.dfs.core.windows.net/gold")
    assert isinstance(p, FileSharePath)
    assert not isinstance(p, LocalFileSharePath)


def test_resolve_fileshare_path_databricks_rejects_non_abfss(set_environment):
    fs = set_environment("databricks")
    with pytest.raises(AssertionError):
        fs.resolve_fileshare_path("/not/an/abfss/path")
```

- [ ] **Step 7: Run test to verify it fails**

Run: `cd framework && uv run pytest tests/unit/test_local_file_share_path.py -v -k resolve_fileshare_path`
Expected: FAIL — `resolve_fileshare_path` still dispatches on the `abfss://` prefix, not `FABRICKS_ENVIRONMENT`, so `test_resolve_fileshare_path_docker_returns_local` fails (still builds a `FileSharePath`) and `test_resolve_fileshare_path_databricks_rejects_non_abfss` fails (no assertion exists yet).

- [ ] **Step 8: Add the dispatch to `resolve_fileshare_path`**

Modify `framework/fabricks/utils/path/file_share.py` — add the import and change the three constructor call sites (lines 144-176):

```python
from fabricks.utils.environment import FABRICKS_ENVIRONMENT
from fabricks.utils.path.local import LocalFileSharePath


def _fileshare_class(value: str) -> type["FileSharePath"]:
    if FABRICKS_ENVIRONMENT == "docker":
        return LocalFileSharePath  # type: ignore[return-value]

    assert value.startswith("abfss://"), (
        f"expected an abfss:// path outside FABRICKS_ENVIRONMENT=docker, got {value!r}"
    )
    return FileSharePath


def resolve_fileshare_path(
    path: str | None,
    default: str | None = None,
    base: FileSharePath | str | None = None,
    variables: dict[str, str] | None = None,
) -> FileSharePath:
    if isinstance(base, str):
        base = _fileshare_class(base)(base)

    resolved_value = path or default
    if resolved_value is None:
        raise ValueError("path and default cannot both be None")

    cls = _fileshare_class(resolved_value)

    if variables:
        return cls.from_uri(resolved_value, regex=variables)

    if base:
        return base.joinpath(resolved_value)

    return cls(resolved_value)
```

(`_fileshare_class` no longer takes a local import for `LocalFileSharePath` — the old shape-sniffing version needed it to sidestep a circular-import concern that doesn't apply here, since `fabricks.utils.environment` has no import relationship with `fabricks.utils.path` at all; both imports move to the top of the file.)

- [ ] **Step 9: Run tests to verify they pass**

Run: `cd framework && uv run pytest tests/unit/test_local_file_share_path.py -v`
Expected: PASS (8 passed)

- [ ] **Step 10: Run the full unit suite to confirm nothing broke**

Run: `cd framework && uv run pytest tests/unit -v`
Expected: all PASS

- [ ] **Step 11: Commit**

```bash
git add framework/fabricks/utils/path/local.py framework/fabricks/utils/path/__init__.py framework/fabricks/utils/path/file_share.py framework/tests/unit/test_local_file_share_path.py
git commit -m "feat: add LocalFileSharePath and URI-scheme dispatch in resolve_fileshare_path

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 6: Minimal local runtime config (databases only, no job configs)

**Files:**
- Create: `framework/tests/local/runtime/fabricks/conf.fabricks.yml`
- Create: `framework/tests/local/__init__.py`
- Test: `framework/tests/local/test_config_loads.py`

**Interfaces:**
- Consumes: `LocalFileSharePath`/`resolve_fileshare_path` dispatch (Task 5), which now also requires `FABRICKS_ENVIRONMENT=docker` (Task 4) to actually return `LocalFileSharePath` instead of `FileSharePath`.
- Produces: env-var contract every later `tests/local/` file relies on — `FABRICKS_BASE`, `FABRICKS_CONFIG`, `FABRICKS_ENVIRONMENT=docker` must all be set (to the values in Step 1 below) **before the first `import fabricks...`** in the test process.

This config registers `bronze`/`silver`/`gold`/`expected` purely as `databases:` entries (`name` + `path_options.storage` — the minimal `Database`/`DatabasePathOptions` model in `fabricks/models/common.py`), not as `bronze:`/`silver:`/`gold:` step configs (`BronzeConf`/`SilverConf`/`GoldConf` — heavier models meant for job orchestration via `get_job()`/`STEPS`, which this plan doesn't use at all). `resolve_runtime_paths()` (`fabricks/models/runtime/utils.py`) treats all four lists (`bronze`, `silver`, `gold`, `databases`) identically for `PATHS_STORAGE` purposes — it only reads `.name`/`.path_options.storage` off each entry — so the plain `databases:` list is sufficient for every CDC object this plan constructs (`Database(database)` in `Configurator.__init__` is a storage-path lookup, not a job-config lookup). No job-config YAML files exist anywhere in this suite.

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/local/test_config_loads.py
import os
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
LOCAL_STORAGE = REPO_ROOT / "framework" / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(REPO_ROOT / "framework")
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"

shutil.rmtree(LOCAL_STORAGE, ignore_errors=True)


def test_bronze_silver_gold_expected_resolve_to_local_storage():
    from fabricks.context import PATHS_STORAGE
    from fabricks.utils.path.local import LocalFileSharePath

    for name in ("bronze", "silver", "gold", "expected"):
        storage = PATHS_STORAGE.get(name)
        assert storage is not None, f"{name} not found in PATHS_STORAGE"
        assert isinstance(storage, LocalFileSharePath)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd framework && uv run pytest tests/local/test_config_loads.py -v`
Expected: FAIL — `tests/local/runtime/fabricks/conf.fabricks.yml` does not exist yet (`FileNotFoundError` surfaced from `yaml.safe_load` in `fabricks/context/runtime.py`).

- [ ] **Step 3: Write the local runtime config**

```yaml
# framework/tests/local/runtime/fabricks/conf.fabricks.yml
---
- conf:
    name: local-test
    options:
      secret_scope: none
      timeouts:
        step: 3600
        job: 3600
        pre_run: 3600
        post_run: 3600
    path_options:
      storage: tests/local/.storage/fabricks
      udfs: fabricks/udfs
      parsers: fabricks/parsers
      schedules: fabricks/schedules
      views: fabricks/views
      requirements: fabricks/requirements.txt
    databases:
      - name: bronze
        path_options:
          storage: tests/local/.storage/bronze
      - name: silver
        path_options:
          storage: tests/local/.storage/silver
      - name: gold
        path_options:
          storage: tests/local/.storage/gold
      - name: expected
        path_options:
          storage: tests/local/.storage/expected
```

(`options.secret_scope`/`options.timeouts` are the only required `RuntimeOptions` fields with no default — `workers`/`retention_days`/`timezone` all default. `path_options.storage`/`udfs`/`parsers`/`schedules`/`views`/`requirements` are the required `RuntimePathOptions` fields with no default — `extenders`/`masks`/`storage_credential`/`variables` are optional. None of `udfs`/`parsers`/`schedules`/`views`/`requirements` need to exist on disk for this plan's scope: `RuntimePathOptions` types them as plain `str`, and `resolve_runtime_paths()` only wraps them in `GitPath`/`FileSharePath` objects — no `.exists()` check happens at config-load time, only if a feature that reads them is actually invoked, which none of this plan's tests do.)

- [ ] **Step 4: Add `.gitignore` entry for the local storage directory**

Modify `framework/.gitignore` (or repo-root `.gitignore` if `framework/` has none — check first with `git check-ignore -v framework/tests/local/.storage` after Step 3 creates no such file yet; add the entry regardless):

```
# Local test storage (Stage 1 CDC/DDL tests) — recreated each run, never committed
framework/tests/local/.storage/
```

- [ ] **Step 5: Create `tests/local/__init__.py`**

```python
# framework/tests/local/__init__.py
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd framework && uv run pytest tests/local/test_config_loads.py -v`
Expected: PASS (1 passed)

- [ ] **Step 7: Commit**

```bash
git add framework/tests/local/runtime framework/tests/local/__init__.py framework/tests/local/test_config_loads.py framework/.gitignore
git commit -m "feat: add minimal local runtime config for Stage 1 CDC tests

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 7: Docker + local Delta-configured SparkSession fixture + first passing test

**Files:**
- Create: `framework/tests/local/Dockerfile`
- Create: `framework/tests/local/docker-compose.yml`
- Create: `framework/tests/local/conftest.py`
- Test: `framework/tests/local/jobs/job1/test_cdc.py`
- Create: `framework/tests/local/jobs/job1/__init__.py`
- Create: `framework/tests/local/jobs/__init__.py`

**Interfaces:**
- Consumes: env-var contract from Task 6, `LocalFileSharePath` from Task 5, `get_spark()`'s `"docker"` branch from Task 4 — `conftest.py` delegates to production `get_spark()` (with `FABRICKS_ENVIRONMENT=docker` set) instead of duplicating the Delta-session-builder logic inline, avoiding two copies of the same `configure_spark_with_delta_pip`/extensions/catalog config drifting apart.
- Produces: a session-scoped `local_spark` pytest fixture (`SparkSession`, Delta-configured, Hive-metastore-backed) that every later `tests/local/` test file depends on by name. Produces a `local-tests` Docker Compose service every later `Run:` instruction for a JVM-dependent test goes through.

**Why the session must be built at module import time, not inside the fixture.** `fabricks/context/__init__.py:41` does `from fabricks.context.spark_session import ... SPARK ...`, and `spark_session.py`'s module-level `SPARK = build_spark_session(app_name="default")` runs unconditionally the instant *anything* is imported from `fabricks.context` — including transitively: `from fabricks.cdc import NoCDC` (this task's own test file) pulls in `fabricks/cdc/base/configurator.py`, which does `from fabricks.context import SPARK` at its own module level. `build_spark_session()` falls back to a plain `SparkSession.builder.getOrCreate()` (no Delta extensions). PySpark's `getOrCreate()` returns the *already-active* JVM session if one exists rather than re-applying `.config(...)` calls — extensions/catalog config only takes effect on a session's first build. So whichever `SparkSession.builder...getOrCreate()` call happens *first* in the process wins, permanently, for the rest of that process. pytest imports a directory's `conftest.py` before collecting any test module in it, which is what makes building the Delta-configured session at `conftest.py`'s module level (not lazily inside `local_spark`'s function body) the fix: it wins the race against every test file's own imports, unconditionally, regardless of which specific test is being run.

**Why Docker.** Per your choice this session: PySpark needs a JVM, and running it in a container (matching the article you linked) avoids depending on whatever JVM happens to be on the host, at the cost of requiring Docker Desktop/daemon and paying container-startup on top of JVM-startup. The image installs `default-jdk-headless` (Debian's JDK metapackage — its `/usr/lib/jvm/default-java` symlink avoids hardcoding an architecture-specific path) plus this repo's Python dependencies via `uv sync`.

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/local/jobs/job1/test_cdc.py
import pytest

from fabricks.cdc import NoCDC


@pytest.mark.order(1)
def test_gold_nocdc_overwrite(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "overwrite", spark=local_spark)

    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.overwrite(df)
    assert nocdc.table.dataframe.count() == 1


@pytest.mark.order(2)
def test_gold_nocdc_append(local_spark):
    df = local_spark.sql("select 1 as dummy")
    nocdc = NoCDC("gold", "nocdc", "append", spark=local_spark)

    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 1
    nocdc.append(df)
    assert nocdc.table.dataframe.count() == 2
```

```python
# framework/tests/local/jobs/__init__.py
```

```python
# framework/tests/local/jobs/job1/__init__.py
```

`delta-spark>=4.1.0` is already a core dependency (`framework/pyproject.toml:21`, `[project.dependencies]`) — no `pyproject.toml` change needed, `uv sync` already installs it.

Note: `delta-spark>=4.1.0` (the 4.x line) paired with `pyspark>=3.5.7` (the 3.5.x line) is a loose version pairing — Delta Lake's compatibility matrix usually ties delta-spark 3.x↔Spark 3.5.x and 4.x↔Spark 4.0.x. `configure_spark_with_delta_pip` (Step 6 below) resolves its jar from the installed `pyspark` version at runtime, so this will likely just work, but if Step 6's `getOrCreate()` fails with a jar/version-mismatch error, check `delta-spark`'s actual installed version against `pyspark`'s and adjust the pin in `[project.dependencies]` rather than assuming compatibility silently.

(The `local` pytest marker is already added by Task 1, Step 4, alongside the `databricks` marker rename — no separate step needed here.)

- [ ] **Step 2: Write the Dockerfile**

```dockerfile
# framework/tests/local/Dockerfile
# Local CDC/DDL tests (tests/local/) — real Spark+Delta, no Databricks.
# Needs a JVM for PySpark; nothing else in this repo does.
# See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir uv

# Keep the venv outside /workspace: docker-compose.yml bind-mounts the repo
# over /workspace at run time, which would otherwise shadow (hide) a venv
# built inside it during `docker build`.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /workspace
COPY pyproject.toml uv.lock ./
RUN uv sync --all-groups --no-install-project

COPY . .
RUN uv sync --all-groups

CMD ["pytest", "tests/local", "-v"]
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
# framework/tests/local/docker-compose.yml
services:
  local-tests:
    build:
      context: ../..
      dockerfile: tests/local/Dockerfile
    volumes:
      - ../..:/workspace
    working_dir: /workspace
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cd framework && docker compose -f tests/local/docker-compose.yml build && docker compose -f tests/local/docker-compose.yml run --rm local-tests pytest tests/local/jobs/job1/test_cdc.py -v`
Expected: FAIL — `fixture 'local_spark' not found` (first build will take a while — installs the JDK and every Python dependency; later runs reuse Docker's layer cache and only rebuild what changed).

- [ ] **Step 5: Write `tests/local/conftest.py`**

```python
# framework/tests/local/conftest.py
"""Conftest for local tests - real Spark+Delta, no Databricks. Applies 'local' marker.

IMPORTANT: the Delta-configured SparkSession below is built here, at this
file's *module import time* — not lazily inside a fixture body. pytest loads
a directory's conftest.py before collecting any test module in it, which is
what makes this win the race against fabricks.context's eager, non-Delta
SPARK singleton (see Task 7's "Why" note in the plan). Do not move this into
`local_spark`'s function body — that reintroduces the race.

Do not mix `tests/local` with `tests/unit`/`tests/databricks` in the same
pytest invocation — `fabricks.context.runtime` resolves CONF_RUNTIME once per
process, and the first import wins.
"""

import os
from pathlib import Path
import shutil

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_LOCAL_STORAGE = _REPO_ROOT / "framework" / "tests" / "local" / ".storage"

os.environ["FABRICKS_BASE"] = str(_REPO_ROOT / "framework")
os.environ["FABRICKS_CONFIG"] = "tests/local/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"

shutil.rmtree(_LOCAL_STORAGE, ignore_errors=True)

from pyspark.sql import SparkSession  # noqa: E402

from fabricks.utils.spark import get_spark  # noqa: E402 - must follow the env-var setup above

_SPARK: SparkSession = get_spark()

from fabricks.metastore.database import Database  # noqa: E402 - must follow _SPARK's construction above

for _db_name in ("bronze", "silver", "gold", "expected"):
    Database(_db_name, spark=_SPARK).create()


def pytest_collection_modifyitems(items):
    """Automatically add 'local' marker to all tests in this directory."""
    root = Path(__file__).parent
    for item in items:
        try:
            if Path(item.fspath).is_relative_to(root):
                item.add_marker(pytest.mark.local)
        except (ValueError, AttributeError):
            if "local" in str(item.fspath):
                item.add_marker(pytest.mark.local)


@pytest.fixture(scope="session")
def local_spark():
    yield _SPARK
    _SPARK.stop()
```

`from fabricks.utils.spark import get_spark` also runs that module's own bottom-of-file `spark = get_spark()` as a side effect of the import (unconditional, module-level, per Task 4) — this is idempotent here, not a second/conflicting session: it reads the same `FABRICKS_ENVIRONMENT=docker` value already set above, builds via the same `"docker"` branch, and `SparkSession.builder...getOrCreate()` returns the already-active session from this file's own explicit `get_spark()` call three lines earlier rather than building a new one.

- [ ] **Step 6: Run test to verify it passes**

Run: `cd framework && docker compose -f tests/local/docker-compose.yml run --rm local-tests pytest tests/local/jobs/job1/test_cdc.py -v`
Expected: PASS (2 passed)

- [ ] **Step 7: Commit**

```bash
git add framework/tests/local/Dockerfile framework/tests/local/docker-compose.yml framework/tests/local/conftest.py framework/tests/local/jobs framework/pyproject.toml
git commit -m "feat: add Dockerized local Delta-configured SparkSession fixture and first passing local CDC test

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 8: Fixture derivation script — job1-9 king/queen bronze rows as NDJSON

**Files:**
- Create: `framework/tests/local/generate_fixtures.py`
- Create: `framework/tests/local/fixtures/job{1..9}/bronze_king_scd1.jsonl` (generated output, committed — 9 files)
- Create: `framework/tests/local/fixtures/job{1..9}/bronze_queen_scd1.jsonl` (generated output, committed — 9 files)
- Test: `framework/tests/unit/test_generate_local_fixtures.py`

**Why the test lives in `tests/unit/`, not `tests/local/`, even though the script it tests lives in `tests/local/`:** `generate_fixtures.py` itself only imports `pandas`/stdlib — no `fabricks`, no Spark. But once Task 7's `tests/local/conftest.py` exists, pytest loads it (and its module-level Delta-session build) for *any* file collected from `tests/local/`, including a test that doesn't use `local_spark` at all — conftest loading is directory-scoped, not test-file-scoped, so there's no way to opt a file in that directory out of it. Placing this test under `tests/unit/` instead avoids the JVM/Docker entirely for a test that has nothing to do with Spark — same reasoning as Task 5's `LocalFileSharePath` test.

**Interfaces:**
- Consumes: raw NDJSON fixtures at `framework/tests/data/job{N}/king/**/*.json` and `framework/tests/data/job{N}/queen/**/*.json` for each job number in `generate_fixtures.py`'s `_JOB_NUMBERS` list (already exist on disk for `job1` through `job11`, read-only — converted from JSON-array to NDJSON format by Task 2 and moved to this shared location by Task 3 — `derive_rows` below reads them with `lines=True`, same as Task 2 updated `convert_json_to_parquet` to do). `_JOB_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9]` for this plan's actual scope — job1 through job9's derived output is generated and committed, verified as the full range where both `king` and `queen` have plain (non-deletelog) data (`job10` has `queen__deletelog` only, `job11` has no queen folder at all — see "Out of scope"). `expected/silver/{scd1,scd2}/job01.sql` through `job09.sql` all exist, confirming the corresponding comparison targets are real.
- Produces: `derive_rows(entity_dir: Path, source: str) -> list[dict]` in `tests/local/generate_fixtures.py` — one dict per input row, with `id`, `name`, `doubleField`, `__timestamp` (ISO string, derived from the containing `YYYY/MM/DD/NNNN` folder path), and `__source` (the `source` argument) keys. `main()` loops over `_JOB_NUMBERS`, calling `derive_rows` per job/entity and writing to `tests/local/fixtures/job{N}/bronze_{entity}_scd1.jsonl` — a naming pattern that already generalizes without a rename, since it was job-number-templated from the start (see docs/adr/0001-...md's job-sequential testing strategy). Used directly by Task 9's bronze-table-loading fixture.

Ported from `tests/databricks/utils.py`'s `convert_json_to_parquet`/`convert_parquet_to_delta`: read each JSON file with `pandas.read_json(..., lines=True)`, derive `__timestamp` from the folder path (`.../YYYY/MM/DD/NNNN/file.json` → `YYYY-MM-DDTHH:00:00`, mirroring the original's `left(concat_ws('', slice(__split, __split_size - 4, 4), '00'), 14)` string-slice logic, using the last 4 path segments before the filename), skip the Unity-Catalog 3x-replication workaround (`for i in range(1, 4)` in the original — irrelevant here), and write NDJSON instead of parquet — both input and output are NDJSON, only the record shape differs (raw landing columns vs. CDC-ready rows).

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/unit/test_generate_local_fixtures.py
import json
from pathlib import Path

from tests.local.generate_fixtures import derive_rows, write_ndjson

_REPO_ROOT = Path(__file__).resolve().parents[3]
_JOB1_KING = _REPO_ROOT / "framework" / "tests" / "data" / "job1" / "king"
_JOB1_QUEEN = _REPO_ROOT / "framework" / "tests" / "data" / "job1" / "queen"


def test_derive_rows_from_job1_king():
    rows = derive_rows(_JOB1_KING, source="king")

    # job1/king has 2 batches (2022/01/01/0001 with 3 rows incl. a duplicate,
    # 2022/01/02/0001 with 1 row) — see the raw fixture files.
    assert len(rows) == 4

    first = rows[0]
    assert first["id"] == 1
    assert first["name"] == "Leopold I"
    assert first["__source"] == "king"
    assert first["__timestamp"] == "2022-01-01T00:01:00"


def test_generated_ndjson_files_are_committed():
    king_path = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures" / "job1" / "bronze_king_scd1.jsonl"
    queen_path = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures" / "job1" / "bronze_queen_scd1.jsonl"

    assert king_path.exists()
    assert queen_path.exists()

    lines = king_path.read_text().strip().splitlines()
    assert len(lines) == 4
    row = json.loads(lines[0])
    assert row["__source"] == "king"


def test_derive_rows_is_deterministic_across_calls():
    # Same call, twice, must produce byte-for-byte identical rows — this
    # output gets committed to git, so any source of nondeterminism (dict
    # ordering, unstable glob ordering across filesystems/OSes) would show
    # up as spurious diffs every time someone regenerates the fixtures.
    first = derive_rows(_JOB1_KING, source="king")
    second = derive_rows(_JOB1_KING, source="king")
    assert first == second


def test_derive_rows_order_matches_sorted_file_path_order():
    # derive_rows sorts entity_dir.rglob("*.json") before reading, so row
    # order should follow the YYYY/MM/DD/NNNN path order chronologically —
    # batch 2022/01/01/0001's 3 rows (incl. a duplicate) before batch
    # 2022/01/02/0001's 1 row. Order matters here: king_and_queen_built
    # (Task 9) feeds these rows straight into NoCDC.overwrite() as one
    # DataFrame, and a silently reordered batch would change which row
    # "wins" a same-key dedup without changing the row count, so a count-only
    # assertion wouldn't catch it.
    rows = derive_rows(_JOB1_KING, source="king")
    timestamps = [r["__timestamp"] for r in rows]
    assert timestamps == sorted(timestamps)


def test_king_and_queen_rows_share_the_same_schema():
    # Task 9's king_and_queen_built fixture unions king's and queen's
    # DataFrames (unionByName). A key mismatch between the two entities'
    # derived rows would only surface there as a confusing Spark error —
    # catching it here, at the pure-Python level, is cheaper and clearer.
    king_rows = derive_rows(_JOB1_KING, source="king")
    queen_rows = derive_rows(_JOB1_QUEEN, source="queen")

    king_keys = {frozenset(r.keys()) for r in king_rows}
    queen_keys = {frozenset(r.keys()) for r in queen_rows}
    assert king_keys == queen_keys, f"schema mismatch: king has {king_keys}, queen has {queen_keys}"


def test_write_ndjson_round_trips_without_loss(tmp_path):
    rows = derive_rows(_JOB1_KING, source="king")
    out_path = tmp_path / "king.jsonl"

    write_ndjson(rows, out_path)
    round_tripped = [json.loads(line) for line in out_path.read_text().splitlines()]

    assert round_tripped == rows


def test_regenerating_fixtures_twice_is_byte_identical(tmp_path):
    # The strongest form of "consistent across iterations": running the
    # full generation twice into two separate directories must produce
    # byte-for-byte identical files, not just equal-when-parsed rows —
    # dict key ordering inside json.dumps() is one way this could silently
    # drift even if test_derive_rows_is_deterministic_across_calls passes.
    rows = derive_rows(_JOB1_KING, source="king")

    first_path = tmp_path / "run1" / "king.jsonl"
    second_path = tmp_path / "run2" / "king.jsonl"
    write_ndjson(rows, first_path)
    write_ndjson(derive_rows(_JOB1_KING, source="king"), second_path)

    assert first_path.read_bytes() == second_path.read_bytes()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd framework && uv run pytest tests/unit/test_generate_local_fixtures.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tests.local.generate_fixtures'`

- [ ] **Step 3: Write `generate_fixtures.py`**

```python
# framework/tests/local/generate_fixtures.py
"""Derive CDC-ready NDJSON rows from the raw JSON landing fixtures under
tests/data/. Run once, output committed to tests/local/fixtures/ —
see docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #6.

Ported from tests/databricks/utils.py's convert_json_to_parquet/
convert_parquet_to_delta (which import databricks.sdk.runtime and can't run
outside a Databricks notebook): same pandas.read_json read, same folder-path
__timestamp derivation, no Spark/parquet write, no Unity-Catalog replication.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DATA_ROOT = _REPO_ROOT / "framework" / "tests" / "data"
_FIXTURES_ROOT = _REPO_ROOT / "framework" / "tests" / "local" / "fixtures"

_DATE_COLUMNS = ["BEL_DeleteDateUtc", "BEL_RestoredDateUtc", "BEL_UpdateDateUtc"]


def _timestamp_from_path(json_file: Path) -> str:
    """Mirror convert_parquet_to_delta's folder-path timestamp derivation:
    the parent dir is .../YYYY/MM/DD/NNNN, giving 'YYYY-MM-DDT00:NN:00' —
    minute-of-day encoded from the batch number, hour fixed at 00, matching
    the original's `slice(__split, __split_size - 4, 4)` over year/month/day/batch.
    """
    parts = json_file.parent.parts
    year, month, day, batch = parts[-4], parts[-3], parts[-2], parts[-1]
    minute = int(batch) % 60
    return f"{year}-{month}-{day}T00:{minute:02d}:00"


def derive_rows(entity_dir: Path, source: str) -> list[dict]:
    rows: list[dict] = []
    for json_file in sorted(entity_dir.rglob("*.json")):
        timestamp = _timestamp_from_path(json_file)
        df = pd.read_json(json_file, orient="records", lines=True, convert_dates=_DATE_COLUMNS)
        for record in df.to_dict(orient="records"):
            record["__timestamp"] = timestamp
            record["__source"] = source
            rows.append(record)
    return rows


def write_ndjson(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")


# job1 through job9: the verified full range where both king and queen have
# plain (non-deletelog) raw data (job10 has queen__deletelog only, job11 has
# no queen folder at all — see "Out of scope"). Adding job10/11 needs new
# design work (delete-log or entity-optional merge handling), not just a
# list entry — see docs/adr/0001-...md's job-sequential strategy.
_JOB_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    for job_num in _JOB_NUMBERS:
        job_dir = f"job{job_num}"
        for entity in ("king", "queen"):
            rows = derive_rows(_DATA_ROOT / job_dir / entity, source=entity)
            write_ndjson(rows, _FIXTURES_ROOT / job_dir / f"bronze_{entity}_scd1.jsonl")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the script to generate the committed fixtures**

Run: `cd framework && uv run python -m tests.local.generate_fixtures`
Expected: creates `tests/local/fixtures/job{N}/bronze_king_scd1.jsonl` and `tests/local/fixtures/job{N}/bronze_queen_scd1.jsonl` for every `N` in `_JOB_NUMBERS` (job1 through job9 — 18 files total).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd framework && uv run pytest tests/unit/test_generate_local_fixtures.py -v`
Expected: PASS (7 passed)

If `test_derive_rows_from_job1_king`'s row count or first-row values don't match — re-check the raw fixture files at `framework/tests/data/job1/king/**/*.json` (read them directly) and adjust the test's expected values to match, rather than the derivation logic to match a guessed count.

- [ ] **Step 6: Commit**

```bash
git add framework/tests/local/generate_fixtures.py framework/tests/local/fixtures framework/tests/unit/test_generate_local_fixtures.py
git commit -m "feat: add fixture derivation script and committed job1-9 NDJSON fixtures

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 9: Silver SCD1/SCD2 test against real fixture data and `expected/**/job*.sql`

**Files:**
- Create: `framework/tests/local/compare.py`
- Modify: `framework/tests/local/conftest.py` (add a session-scoped fixture that builds bronze/silver tables)
- Test: `framework/tests/local/jobs/job1/test_silver.py`

**Interfaces:**
- Consumes: `derive_rows`/NDJSON fixtures (Task 8), `local_spark` fixture (Task 7), `assert_dfs_equal` from `tests.databricks.compare` (unchanged import).
- Produces: `create_expected_views(spark, step: str, cdc: str) -> None` and `compare_silver_to_expected(spark, table, cdc: str, iter: int, topic: str) -> None` in `tests/local/compare.py` — local equivalents of `tests/databricks/utils.py::create_expected_views` and `tests/databricks/compare.py::compare_silver_to_expected`, parameterized on `spark` instead of the Databricks-runtime global, and on a `Table` instead of a `BaseJob` (this plan has no job objects at all — see below).
- Produces: a session-scoped `king_and_queen_built` **fixture factory** in `conftest.py` — not a fixture that directly returns built tables, but one that returns a callable, `_build(jobs: list[int]) -> dict`. `_build` applies each job number in `jobs`, **in order**, as its own separate `SCD1`/`SCD2.update()` call (not one call over every job's rows unioned together — see "Why job-sequential, not batch-sequential" below), so job N's merge genuinely runs against whatever table state job N-1's merge left behind. `test_silver.py` calls this factory once per parametrized scenario.

**Why job-sequential, not raw-batch-sequential.** `scd2.sql.jinja`'s `__scd2_base` CTE derives `__valid_from`/`__valid_to` via `lead(__timestamp) over (partition by __key order by __timestamp)` — a window function that already correctly historizes same-key rows by timestamp *within a single `.update()` call*, regardless of how many raw landing-JSON batch files contributed those rows (confirmed: job1/king has 2 raw batches, job1/queen has 3, and one `.update()` call over all of job1's rows together produces the correct 2-row history for king id=2). So the raw `YYYY/MM/DD/NNNN` batch-file boundary does *not* need sequential `.update()` calls — and splitting per raw-batch-file would actually be wrong (`correct_valid_from`'s epoch-start rewrite is a global minimum over the whole call's result set; calling it more than once would misapply it per-batch). What *does* need sequencing is the **job** boundary (`job1`, `job2`, `job3`, ... — each representing a separate real-world run): each job's `.update()` call needs to run against the *already-merged* state from the previous job's call, which is what exercises the incremental-merge-into-an-already-populated-table code path (`Processor.get_query_context()`'s `has_rows` branch, gated by `{% if has_rows %}` in the templates) — a code path a single-job scenario never touches, since that job's table starts empty. This plan's actual scope covers every contiguous prefix `[1]` through `[1..9]` (see `_SCENARIOS` below); the mechanism is job-sequential-general so a future scenario starting elsewhere (e.g. `jobs=[2, 3]`, skipping job1 entirely — a different starting point, not always "replay from job1 forward") is a new `_SCENARIOS` tuple, not a redesign. Chains beyond `jobs=[1]` also exercise `autoMerge` (Task 4): job2's raw data introduces a new column (`newField`, verified present through job9) job1's table schema doesn't have yet.

No job objects exist in this plan, so `compare.py`'s original `job.mode == "memory"` branch and `str(job)` (`BaseJob.__str__`) don't apply — this local version takes the already-materialized `Table` directly instead of a job wrapper, since a bare `SCD1`/`SCD2` CDC object's `.table` property (`Configurator.table`) already gives exactly that.

- [ ] **Step 1: Write the failing test**

```python
# framework/tests/local/jobs/job1/test_silver.py
import pytest

from tests.local.compare import compare_silver_to_expected

# (jobs, compare_to): jobs are applied in order, each via its own
# SCD.update() call; compare_to is the expected/**/job{N}.sql this
# scenario's final state should match. Every contiguous prefix of job1
# through job9 (the full range Task 8 derives) — exercises both a
# from-empty-table bulk load (jobs=[1]) and the incremental-merge-into-an-
# already-populated-table code path every jobs=[..., N] entry beyond the
# first job adds. A scenario that *skips* job1 (e.g. jobs=[2, 3], comparing
# to job03) is a different starting point, not a longer chain — deliberately
# not included here since job2 alone was never verified as its own valid
# starting schema (job1 is what establishes the base columns every later
# job's autoMerge-handled drift builds on); add such an entry only after
# confirming job2's raw data is independently mergeable into an empty table.
# See king_and_queen_built's "Why job-sequential" note for why the job
# boundary (not the raw landing-batch boundary) is what needs sequencing.
_SCENARIOS = [
    ([1], 1),
    ([1, 2], 2),
    ([1, 2, 3], 3),
    ([1, 2, 3, 4], 4),
    ([1, 2, 3, 4, 5], 5),
    ([1, 2, 3, 4, 5, 6], 6),
    ([1, 2, 3, 4, 5, 6, 7], 7),
    ([1, 2, 3, 4, 5, 6, 7, 8], 8),
    ([1, 2, 3, 4, 5, 6, 7, 8, 9], 9),
]


@pytest.mark.order(10)
@pytest.mark.parametrize("jobs,compare_to", _SCENARIOS)
def test_silver_king_and_queen_scd2(local_spark, king_and_queen_built, jobs, compare_to):
    built = king_and_queen_built(jobs)
    compare_silver_to_expected(local_spark, table=built["scd2"].table, cdc="scd2", iter=compare_to, topic="king_and_queen")


@pytest.mark.order(11)
@pytest.mark.parametrize("jobs,compare_to", _SCENARIOS)
def test_silver_king_and_queen_scd1(local_spark, king_and_queen_built, jobs, compare_to):
    built = king_and_queen_built(jobs)
    compare_silver_to_expected(local_spark, table=built["scd1"].table, cdc="scd1", iter=compare_to, topic="king_and_queen")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd framework && docker compose -f tests/local/docker-compose.yml run --rm local-tests pytest tests/local/jobs/job1/test_silver.py -v`
Expected: FAIL — `fixture 'king_and_queen_built' not found`

- [ ] **Step 3: Write `tests/local/compare.py`**

```python
# framework/tests/local/compare.py
"""Local (spark-injected) equivalents of tests/databricks/compare.py's
SPARK-global-coupled comparison helpers. assert_dfs_equal itself doesn't
touch the SPARK global, so it's reused unchanged.

See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #7.
"""

import json
from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from fabricks.metastore.table import Table
from tests.databricks.compare import assert_dfs_equal

_EXPECTED_ROOT = Path(__file__).resolve().parents[1] / "expected"  # tests/expected/ — shared sibling, see Task 3

_EXPECTED_SCD2_SCHEMA = StructType(
    [
        StructField("__valid_from", TimestampType(), True),
        StructField("__valid_to", TimestampType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("doubleField", DoubleType(), True),
        StructField("__is_current", BooleanType(), True),
        StructField("__is_deleted", BooleanType(), True),
        StructField("__source", StringType(), True),
    ]
)


def create_expected_views(spark: SparkSession, step: str, cdc: str) -> None:
    views_dir = _EXPECTED_ROOT / step / cdc

    if step == "silver" and cdc == "scd2":
        # Only job1's file is hand-authored NDJSON data — job2.sql onward are
        # still real SQL, each unioning its own new VALUES rows with `select
        # ... from expected.silver_scd2_job{N-1} where not __is_current`
        # (verified: job3.sql references job2, job2.sql references job1 — a
        # genuine sequential chain). Create job1's NDJSON root, then fall
        # through to the SQL loop below for job2 onward in ascending order —
        # an early `return` here would silently skip
        # expected.silver_scd2_job{2..9} entirely, since Task 9's
        # king_and_queen_built only calls create_expected_views for
        # ("silver", "scd2")/("silver", "scd1"), not per job. See Task 3's
        # mirrored fix in tests/databricks/utils.py's create_expected_views.
        for ndjson_file in sorted(views_dir.glob("*.jsonl")):
            job_num = re.search(r"\d+", ndjson_file.stem).group()
            rows = [json.loads(line) for line in ndjson_file.read_text().splitlines()]
            df = spark.createDataFrame(rows, schema=_EXPECTED_SCD2_SCHEMA)
            df.write.mode("overwrite").saveAsTable(f"expected.silver_scd2_job{job_num}")

    for sql_file in sorted(views_dir.glob("*.sql")):
        spark.sql(sql_file.read_text())


def compare_silver_to_expected(spark: SparkSession, table: Table, cdc: str, iter: int, topic: str) -> None:
    df = table.dataframe

    expected_df = spark.read.table(f"expected.silver_{cdc}_job{iter}")
    if topic in ["monarch", "memory", "regent"]:
        expected_df = expected_df.drop("__source")

    assert_dfs_equal(df, expected_df)
```

- [ ] **Step 4: Add the `king_and_queen_built` fixture to `conftest.py`**

Modify `framework/tests/local/conftest.py` — append after the `local_spark` fixture:

```python
@pytest.fixture(scope="session")
def king_and_queen_built(local_spark):
    """Factory fixture: returns a callable that builds bronze/silver tables
    for a given ordered list of job numbers, one SCD1/SCD2.update() call per
    job (see Task 9's "Why job-sequential" note) — no job classes, no job
    config, no get_job(). Each scenario's tables are named by its job-list
    signature (e.g. jobs=[1] -> topic "king_and_queen_1", jobs=[1, 2] ->
    "king_and_queen_1_2") so multiple parametrized scenarios in the same
    pytest session get isolated tables, no state collision.

    Outer fixture is session-scoped (creates the `expected` views once,
    idempotently); the returned `_build` callable is invoked per test with
    that test's own `jobs` list.
    See docs/adr/0001-duckdb-backend-for-local-cdc-tests.md, Stage 1 item #5.
    """
    import json

    from fabricks.cdc.nocdc import NoCDC
    from fabricks.cdc.scd1 import SCD1
    from fabricks.cdc.scd2 import SCD2

    from tests.local.compare import create_expected_views

    create_expected_views(local_spark, "silver", "scd2")
    create_expected_views(local_spark, "silver", "scd1")

    def _build(jobs: list[int]) -> dict:
        suffix = "_".join(str(j) for j in jobs)
        scd2 = SCD2("silver", f"king_and_queen_{suffix}", "scd2", spark=local_spark)
        scd1 = SCD1("silver", f"king_and_queen_{suffix}", "scd1", spark=local_spark)

        for job_num in jobs:
            fixtures_root = Path(__file__).resolve().parent / "fixtures" / f"job{job_num}"

            job_dfs = []
            for entity in ("king", "queen"):
                rows = [
                    json.loads(line)
                    for line in (fixtures_root / f"bronze_{entity}_scd1.jsonl").read_text().splitlines()
                ]
                df = local_spark.createDataFrame(rows)
                # append, not overwrite: bronze accumulates across jobs,
                # matching production's landing-table semantics. The SILVER
                # merge below uses this job's own `df` directly, not
                # nocdc.table.dataframe (which would be every job's rows
                # accumulated so far) — each job's .update() call gets only
                # that job's new rows; the CDC merge itself is what compares
                # against the already-merged silver state from prior jobs.
                nocdc = NoCDC("bronze", f"{entity}_{suffix}", "scd1", spark=local_spark)
                nocdc.append(df)
                job_dfs.append(df)

            combined = job_dfs[0].unionByName(job_dfs[1], allowMissingColumns=True)

            # One .update() call per job, sequentially: job N's call runs
            # against whatever table state job N-1's call left behind.
            scd2.update(combined, keys=["id"])
            scd1.update(combined, keys=["id"])

        return {"scd1": scd1, "scd2": scd2}

    return _build
```

`__key`/`__hash`/`__merge_key`/`__merge_condition` are computed internally by `SCD.update()` → `Merger.merge()` → `Processor.get_query_context()` from `keys=["id"]` and the non-`__`-prefixed columns of `combined` (`id`, `name`, `doubleField`) — confirmed by reading `Processor.get_query_context()` (`fabricks/cdc/base/processor.py`): `keys = keys if keys is not None else list(fields)`, with `__source` auto-appended to `keys` when `has_source` is true (it is here, since both bronze tables carry `__source`). Do not pre-compute these columns in the fixture. This holds per-job the same way it held for the single-call version — each `.update()` call still receives only raw, non-`__merge_key`-bearing columns.

Add the `import json`, `from pathlib import Path` (already imported at module level from Task 7), `from fabricks.cdc.nocdc import NoCDC`, `from fabricks.cdc.scd1 import SCD1`, `from fabricks.cdc.scd2 import SCD2`, `from tests.local.compare import create_expected_views` — place the `json`/`NoCDC`/`SCD1`/`SCD2`/`create_expected_views` imports inside the fixture function (as shown above) rather than at module level, since they trigger `fabricks` submodule imports that must happen after the env vars at the top of the file are set — consistent with how `local_spark` already does its `fabricks` imports inside the fixture body, not at module scope.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd framework && docker compose -f tests/local/docker-compose.yml run --rm local-tests pytest tests/local/jobs/job1/test_silver.py -v`
Expected: PASS (18 passed) — 2 test functions × 9 parametrized scenarios (`_SCENARIOS`, `jobs=[1]` through `jobs=[1..9]`).

If a specific scenario fails, isolate it first (`pytest tests/local/jobs/job1/test_silver.py -k "jobs3-compare_to3"` or similar `-k` filter on the parametrize id, rather than re-running all 18) before investigating. For a row-count/column-value mismatch against `expected.silver_{scd1,scd2}_job{compare_to}`, compare what that expected view actually contains (`framework/tests/expected/silver/scd1/job{NN}.sql`'s query, or `job01.jsonl`'s rows for job1's root) against what `SCD1`/`SCD2.update()` produces — the most likely mismatches are `keys=` (confirm the real `king_and_queen` job's business key really is just `id`, by checking whether the raw fixture data has any per-entity id collisions between king and queen that only `__source` disambiguates — if so, `keys=["id"]` combined with the automatic `__source` append already handles it, per the paragraph above), a `__timestamp` derivation mismatch from Task 8, or — for any scenario beyond `jobs=[1]` — a genuine schema-drift case `autoMerge` doesn't cover (a type change rather than a new column; `autoMerge` handles the latter, confirmed present via job2's `newField`, but not the former). Don't adjust `expected/**/job{NN}.sql` to match a guessed output — that file is the correctness oracle.

If a *future* scenario (e.g. `jobs=[1, 2]`) produces the wrong history once job2's fixture data exists, check first whether job2's `_build` call is actually landing against job1's already-merged table state (i.e. that `_build`'s per-job loop isn't accidentally re-creating the `SCD1`/`SCD2` objects inside the loop instead of once per scenario, which would reset `has_rows` to falsy for every job and silently degrade back to the single-job code path) before suspecting the merge logic itself.

- [ ] **Step 6: Commit**

```bash
git add framework/tests/local/compare.py framework/tests/local/conftest.py framework/tests/local/jobs/job1/test_silver.py
git commit -m "feat: add local Silver SCD1/SCD2 test against real fixture data and expected views

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 10: Document the new suite

**Files:**
- Modify: `framework/docs/TEST.md`
- Modify: `framework/AGENTS.md` (only if `docs/TEST.md`'s pointer line needs updating — check first; it likely doesn't, since `AGENTS.md` already points at `TEST.md` generically)

**Interfaces:** None — documentation only.

- [ ] **Step 1: Add a "Local tests" section to `TEST.md`**

Insert a new `## Local tests — tests/local/` section between the existing `## Unit tests` and `## Integration tests` sections (`docs/TEST.md`), covering: what it's for (real local Spark+Delta CDC/DDL testing, no Databricks), that it requires Docker (Docker Desktop or an equivalent daemon — PySpark needs a JVM, and this suite runs it in a container rather than depending on the host's; build once with `cd framework && docker compose -f tests/local/docker-compose.yml build`, then run with `docker compose -f tests/local/docker-compose.yml run --rm local-tests pytest tests/local -v`), never combined with `tests/unit`/`tests/databricks` in the same invocation (see Task 6's/Task 7's Global Constraints), how fixtures are generated (`uv run python -m tests.local.generate_fixtures` — plain host command, no Docker needed, since fixture generation itself has no Spark dependency — output committed, regenerate deliberately on a fixture-data change, don't hand-edit the NDJSON), and a pointer to `docs/adr/0001-duckdb-backend-for-local-cdc-tests.md` for the full design rationale.

- [ ] **Step 2: Update `TEST.md`'s "Rule of thumb" section**

Modify the existing "## Rule of thumb" section to mention the new suite as a third option alongside unit/integration, per the actual final wording once Step 1's section exists (read the file's current end-state before editing, to phrase this consistently with the surrounding prose rather than as a bolted-on addendum).

- [ ] **Step 3: Commit**

```bash
git add framework/docs/TEST.md
git commit -m "docs: document the new tests/local/ suite

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Out of scope for this plan (see ADR 0001 for the full design)

- Stage 2 (DuckDB backend, Narwhals, merge-template rewrite, type-mapping table) — deferred entirely.
- Parquet per-checkpoint snapshot materialization and the fast-parallel / slow-sequential dual-mode split (ADR Stage 1 item #8) — a natural second plan once this sequential suite exists and its slowness is actually felt, not before.
- **job10 and job11** (and any job beyond `gold.nocdc`/`silver.king_and_queen`) — genuinely out of scope, not just deferred: verified directly against the raw fixture files, `queen` has no plain (non-deletelog) data past job9 (`job10` has `queen__deletelog` only, `job11` has no queen folder at all). Extending past job9 needs real design work this plan doesn't do — either delete-log handling (`king__deletelog`/`queen__deletelog`) or an entity-optional merge path (a job whose `.update()` call only carries one entity's rows, with the other's state simply carrying forward from whichever earlier job last touched it) — not just a `_JOB_NUMBERS`/`_SCENARIOS` entry.
- **Scenarios that skip job1** (e.g. `jobs=[2, 3]`, comparing to job03) — the mechanism supports this (any job list, in order), but no such scenario is populated in `_SCENARIOS`: job2's raw data was never verified as independently mergeable into an *empty* table (job1 is what establishes the base schema every later job's `autoMerge`-handled drift builds on). Adding one is a new `_SCENARIOS` tuple once that's checked, not a mechanism change.
- Type-changing schema drift (as opposed to a new column) between job1 and job9 — `autoMerge` (Task 4) is confirmed to handle job2's new `newField` column, but a genuine column type change (not verified absent across job3-job9's raw data) would need `update_schema(widen_types=True)`-style handling `autoMerge` alone doesn't provide. Flagged as an open risk, not ruled out.
- CI wiring for the new suite.
- DDL snapshot-testing (`Table._create`'s generated SQL text) — this plan only covers CDC merge SQL execution; DDL snapshot tests are a separate, independent piece of ADR 0001's scope split that doesn't depend on anything built here.
- The `Bronze`/`Silver`/`Gold` job-orchestration layer itself (job-config YAML, `get_job()`, `STEPS`, dependency lineage, streaming ingestion) — this plan tests the CDC layer directly, not job orchestration. If job orchestration itself ever needs local test coverage, that's new scope, not an extension of this plan.

## Self-Review

**1. Spec coverage.** ADR 0001 Stage 1 items and where each is covered:
- Item 1 (local Delta-configured SparkSession) → Task 7, built at `conftest.py` module import time (not fixture-lazily) to close the session-ordering race against `fabricks.context`'s eager, non-Delta `SPARK` singleton; the Delta-session-building logic itself lives in production `get_spark()`'s `"docker"` branch (Task 4), not duplicated in the fixture.
- Item 2 (`LocalFileSharePath`) → Task 5.
- Item 3 (fixture reuse via ported `convert_json_to_parquet`/`convert_parquet_to_delta` logic) → Task 8.
- Item 4 (table creation needs nothing new) → Task 7 (`NoCDC.overwrite` auto-creates) and Task 9 (same, for bronze/silver tables, plus `SCD1`/`SCD2.update()` auto-creating the silver tables) — no separate task needed, confirmed by using the existing `.overwrite()`/`.update()` paths directly rather than writing new creation code, and no job-orchestration layer needed at all for this (a stronger form of "nothing new" than originally anticipated).
- Item 5 (session-scoped table build, sequential replay) → Task 9's `king_and_queen_built` fixture factory, refined this round from "full union, one merge call" to "one merge call per job, sequential" — see the "job-sequential vs. raw-batch-sequential" clarification below and in Task 9's own prose.
- Item 6 (pre-derived NDJSON fixtures, git-committed) → Task 8 (generation script in `tests/local/`, its test relocated to `tests/unit/` — see Task 8's "Why" note).
- Item 7 (reuse `expected/**/job*.sql` + `compare.py`) → Task 9.
- Item 8 (Parquet checkpoint snapshots, parallel execution) → explicitly out of scope (documented above), per the brief.
- DDL scope split (snapshot-tested as text) → explicitly out of scope (documented above) — this plan only covers CDC merge SQL, not DDL generation testing.
- Bronze ingestion out of scope → honored throughout, and more thoroughly than the first draft: Task 9 never constructs a `Bronze` job object at all, only `NoCDC` directly.
- Docker (this turn's addition, not an ADR item) → Task 7's `Dockerfile`/`docker-compose.yml`, with `Run:` instructions in Tasks 7/8/9 routed through `docker compose run`; Tasks 5 and 8 (relocated) stay plain host commands since neither touches Spark.
- `tests/integration/` → `tests/databricks/` rename (a later turn's addition, not an ADR item — a naming-clarity request, not a design decision) → Task 1, sequenced first since every later task's `tests/databricks/...` references assume it already happened.
- SparkSession session-ordering fix (this turn's addition, not an ADR item — a bug found while reworking the plan, not a design decision) → Task 7's `conftest.py`, module-level session construction, with the mechanism explained inline as a comment so a future editor doesn't "simplify" it back into the fixture body.
- Raw fixture NDJSON conversion (a later turn's addition, not an ADR item — a format-consistency request matching Task 8's already-NDJSON derived output, not a design decision) → Task 2, sequenced right after the rename since Task 8 depends on these files already being NDJSON by the time it reads them; also fixes `convert_json_to_parquet`'s reader (`tests/databricks/utils.py`) and `derive_rows`' reader (Task 8) to match.
- `data/`/`expected/` extracted to shared top-level `tests/data/`/`tests/expected/` siblings (a later turn's addition, not an ADR item — a coupling-removal request, not a design decision) → Task 3, sequenced after Task 2 (moves already-NDJSON files) and before Task 8 (which reads from the new location); also fixes `tests/databricks/utils.py`'s two `paths.tests.joinpath(...)` call sites (`git_to_landing`/`create_expected_views`) to `paths.tests.parent.joinpath(...)`, and Task 8/Task 9's path references (`_DATA_ROOT`, `_JOB1_KING`, `_EXPECTED_ROOT`) to match.
- `silver/scd2/job01`'s expected-data root converted from `.sql` (`values (...)`) to `.ndjson` (a later turn's addition, not an ADR item — a readability request, not a design decision) → folded into Task 3 (Step 2b), not a separate task, since Task 3 already touches `create_expected_views()` for the path fix and this is the same function. Only the DAG root converts — every derived file (`silver/scd1`, `gold/scd0`/`scd1`/`scd2`) stays SQL, referencing `expected.silver_scd2_job{N}` by name, unaffected. Both `create_expected_views()` implementations (`tests/databricks/utils.py`, Task 3; the local port in `tests/local/compare.py`, Task 9) special-case `step=="silver" and cdc=="scd2"` identically, so there's exactly one source of truth for job1's root data shared by both suites, not a `.sql`/`.ndjson` pair that could drift against each other.
- `DATABRICKS_LOCALMODE` replaced with a 3-way `FABRICKS_ENVIRONMENT` (`docker`/`remote`/`databricks`) (a later turn's addition, not an ADR item — the old boolean conflated "native Databricks cluster" and this plan's fully-local Docker test environment under the same `false` value, a real environment-modeling gap, not a naming preference) → new Task 4, sequenced before Task 5 (`resolve_fileshare_path`'s dispatch now reads this instead of sniffing `abfss://`) and before Task 7 (`local_spark` now delegates to production `get_spark()`'s new `"docker"` branch instead of duplicating it). Default (`"databricks"`) preserves existing behavior for every caller that sets nothing — verified low blast radius (3 files reference the old flag) before committing to a clean replace over a compatibility shim.

- Job-sequential testing strategy (this turn's addition, refining Item 5 — not contradicting it, since job1's own history was already correct; the refinement is about what happens *across* jobs) → Task 8's `_JOB_NUMBERS` list and Task 9's `king_and_queen_built(jobs)` factory + `test_silver.py`'s `_SCENARIOS` parametrize list. Verified against the real merge templates before writing this in: `scd2.sql.jinja`'s `lead(__timestamp) over (partition by __key order by __timestamp)` already historizes correctly across every row in one `.update()` call regardless of raw-batch-file count, so only the *job* boundary needs sequential calls, not the raw landing-batch boundary — confirmed by tracing `correct_valid_from`'s global-minimum rewrite, which would be *wrong* if reapplied per raw batch. This plan's actual scope is `_JOB_NUMBERS = [1, ..., 9]` and every contiguous prefix `_SCENARIOS = [([1], 1), ..., ([1..9], 9)]` — the full verified range where both `king` and `queen` have plain data; job10/11 are out of scope because `queen`'s data doesn't extend that far, not because the mechanism can't.
- `autoMerge` schema-drift handling (this turn's addition, a consequence of extending past job1 — a real design decision, not mechanical) → Task 4's `get_spark()` `"docker"` branch: `spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = true")`, added after verifying (a) job2's raw data genuinely introduces a new column (`newField`, persists through job9) that a job1-created table schema wouldn't have, since this plan bypasses job orchestration's own `update_schema()` calls entirely, and (b) the setting is a core OSS Delta Lake feature (available since Delta 0.6.0, confirmed via Delta Lake's own documentation), not Databricks-Runtime-only despite the `spark.databricks.*` config namespace — so it works identically in the Dockerized local session. Matches what production's `add_spark_options_to_spark()` (`fabricks/context/spark_session.py`) already sets for every real Databricks session, rather than inventing local-only reconciliation logic. `resolveMergeUpdateStructsByName` (the other setting `add_spark_options_to_spark()` sets) deliberately not added alongside it — this plan's fixture data has no `__metadata`/struct columns (verified against `has_metadata = "__metadata" in columns`, `fabricks/cdc/base/processor.py`), so the struct-field merge clause that setting affects is never emitted for any of this plan's scenarios.
- **Bug fix surfaced by extending scope past job1** (found while writing this round's changes, not pre-existing in the ADR): both `create_expected_views()` implementations' `step=="silver" and cdc=="scd2"` branch (`tests/databricks/utils.py`, Task 3; `tests/local/compare.py`, Task 9) had an early `return` after processing only `.ndjson` files. This was invisible while only job1 existed (job1's `.sql` file was already converted to `.ndjson`, so there was nothing left in that directory for the early return to skip) — but job2.sql through job9.sql were verified (read directly: job2.sql references `expected.silver_scd2_job1`, job3.sql references `expected.silver_scd2_job2` — a genuine sequential chain, each with its own hand-authored `VALUES` rows, not a pure derived view) to still be real, necessary `.sql` files in that same directory. The early `return` would have silently skipped creating `expected.silver_scd2_job{2..9}` entirely, breaking every scenario beyond `jobs=[1]`. Fixed in both implementations: create job1's NDJSON root, then fall through (no `return`) to the existing SQL loop for job2 onward, in ascending job-number order (`sorted()` on zero-padded filenames already gives the correct sequential order the chain requires).

**2. Placeholder scan.** Task 9 Step 3 originally included a "placeholder removed below" stub for illustration of why a naive first attempt at `create_expected_views` fails — replaced immediately by the final, complete version in the same step. No other TBD/TODO/"add appropriate"/unshown-code steps remain.

**3. Type consistency.** `LocalFileSharePath` (Task 5) constructor signature `(path: str | PathlibPath)` matches `FileSharePath`/`GitPath`/`BasePath`'s. `resolve_fileshare_path`'s return type annotation stays `FileSharePath` even though it may now return a `LocalFileSharePath` instance (a `BasePath` subclass, structurally compatible with every call site's usage of `.joinpath()`/`.exists()`/etc.) — flagging this as a known, deliberate type-hint imprecision rather than a bug: tightening it to a union or a `BasePath` return type is a one-line follow-up if a type checker complains, not blocking for this plan. `create_expected_views`/`compare_silver_to_expected` (Task 9) match the signatures declared in Task 9's Interfaces block — note `compare_silver_to_expected` takes a `Table` (`table=`) and a separate `topic=` string, not a `BaseJob`, since this plan has no job objects; `test_silver.py` (Task 9, Step 1) calls it accordingly (`table=scd2.table, ..., topic="king_and_queen"`). The `king_and_queen_built` fixture's returned dict keys (`"scd1"`, `"scd2"`) hold `SCD1`/`SCD2` CDC objects, not job objects, matching how `test_silver.py` uses them (`.table` attribute access, not job methods). `local_spark` (Task 7) is now a thin fixture wrapping the module-level `_SPARK` object built by `conftest.py`'s top-level code — every consumer (`test_cdc.py`, the `king_and_queen_built` fixture) still requests it as `local_spark` via normal pytest dependency injection, so no call site elsewhere in the plan changes. `_EXPECTED_SCD2_SCHEMA` is defined identically in both `create_expected_views()` implementations (Task 3's `tests/databricks/utils.py`, Task 9's `tests/local/compare.py`) — same eight fields, same types, same order — since both load the same `job01.ndjson` file; the two use different file-listing calls appropriate to their own context (`GitPath.walk(file_format="ndjson")` in the `tests/databricks/` version, since it already used `GitPath`/`.walk()` for the SQL case; plain `Path.glob("*.ndjson")` in the `tests/local/` version, since it already used `Path`/`.glob()` for its SQL case) — not an inconsistency, each matches the surrounding code's existing idiom. `FabricksEnvironment`/`FABRICKS_ENVIRONMENT` (Task 4, `fabricks.utils.environment`) is imported identically by `utils/spark.py` (Task 4) and `utils/path/file_share.py` (Task 5) — same three literal values (`"docker"`/`"remote"`/`"databricks"`), same module, no second definition anywhere to drift.

**4. Task boundaries.** Task 4 (`FABRICKS_ENVIRONMENT`) was inserted as a new task between Task 3 and (old) Task 4, pushing old Tasks 4-9 to 5-10 — not folded into Task 5 (`LocalFileSharePath`) even though Task 5 is its only same-plan consumer, because Task 4's actual deliverable (a rewritten `get_spark()`/`get_dbutils()`/`display()` in production `utils/spark.py`) is independently testable and has its own consumer besides Task 5: Task 7's `local_spark` fixture, which comes two tasks later. Merging it into Task 5 would make Task 5's title/scope misleading (a path-class task quietly also rewriting session-building code) and couple two deliverables a reviewer might reasonably want to approve independently. Task 6 and Task 7 stayed separate, not merged — Task 6's test (`from fabricks.context import PATHS_STORAGE`) is exercised standalone, before `conftest.py` exists, so there's no session to race against yet at that point in the plan's sequence; the ordering hazard only exists once Task 7 introduces a Delta-specific session requirement, and Task 7 is where it's closed. Task 8 shifted its test location (`tests/local/` → `tests/unit/`) but not its script/fixture-output location — this is a one-file move, not a task split. Task 1 (the rename) was inserted at the front, pushing what were Tasks 1-6 to Tasks 2-7 — it has no dependency on any other task (a plain filesystem move plus find/replace) and every later task's `tests/databricks/...` path references were written assuming it already ran, so front-loading it is the only ordering that keeps every other task's file paths correct as written. Task 2 (the raw-fixture NDJSON conversion) was inserted immediately after it, pushing those Tasks 2-7 to 3-8 — it depends on Task 1 having already renamed the directory (its own paths are `tests/databricks/...`) and must precede Task 8, which reads these same raw files, so it's sequenced as early as its Task 1 dependency allows. Task 3 (extracting `data/`/`expected/` to shared top-level siblings) was inserted immediately after that, pushing what were Tasks 3-8 to 4-9 — it depends on Task 2 having already converted the raw files to NDJSON (moves a format it doesn't itself need to touch) and must precede Task 8, which reads `data/` from the post-move `tests/data/` location and Task 9, whose `compare.py` reads `expected/` from `tests/expected/` — so, like Task 2, it's sequenced as early as its own dependency allows. The job-sequential generalization (this turn) was folded into Task 8 and Task 9 in place, not given a new task — both changes are direct reworks of a deliverable those tasks already own (Task 8's `main()`, Task 9's fixture and test), not a new independent deliverable with its own consumer the way Task 4 was; a new task here would just be Task 8/9 split across two documents for no reason.
