# Semblance Test Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give contributors a one-fixture way to write local tests against the Databricks/Azure environment, and fix the process-wide mock-state leaks in issue #220.

**Architecture:** Two changes. Change 1 removes the import-time side effects that force wholesale `sys.modules` fakes (Step 0 seams A1/A2) and fixes the five leaking spots. Change 2 adds `tests/semblance/`: strict in-memory Azure Table/Queue fakes, a signature-checked `dbutils` fake, and a function-scoped `semblance` fixture with a small handle API, then migrates the hand-rolled fakes onto it.

**Tech Stack:** Python 3.11, pytest + `monkeypatch`, `azure-data-tables` 12.7.0, `azure-storage-queue` 12.15.0, `databricks-sdk` 0.108.0 (`dbutils_stub`), pyspark 4.1.1, `uv`/`just`.

**Spec:** `docs/superpowers/specs/2026-09-29-local-test-runtime-harness-design.md` (issue: fabricks-framework/fabricks#220).

## Global Constraints

- One tier per pytest process: never mix `tests/unit/plain`, `tests/unit/config`, `tests/spark/apache`, `tests/spark/databricks` in one invocation. Run tiers with `just test-plain`, `just test-config`, `just test-apache` from `framework/`.
- No new dependencies. No Docker anywhere. Azurite is opt-in via `npx azurite`, never required by default runs.
- `tests/semblance/*.py` must not import `fabricks` at module level (the fixture plugin loads at collection, before tier conftests bootstrap the fake environment).
- Fakes fail loudly: unsupported operations raise (`NotImplementedError`/`AttributeError`/`TypeError`), never return a `MagicMock`.
- Fake filter grammar is exactly `Field eq 'value'` clauses joined by ` and `, or the empty string.
- Reference for `dbutils` signatures is the pinned SDK (`databricks-sdk` 0.108.0 in `uv.lock`), loaded by file path.
- Run `just format` and `just lint` (from `framework/`) before every commit; the repo pre-commit hook does the same.
- All commands below run from `framework/` unless stated. Commit messages end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.

## Deviations from the spec (the spec has been updated to match)

- Seeding goes through plain dicts on the handle (`semblance.widgets`, `.secrets`, `.task_values`) and `semblance.on_notebook_run(...)`, **not** `semblance.dbutils.widgets.set(...)`: real `dbutils` has no `widgets.set`, and the conformance test forbids offering methods the stub lacks.
- The fixture patches `TABLE_LOG_HANDLER._table` (no property setter): `monkeypatch.setattr` reads the old value through the property, which would resolve the factory and fail locally.
- Fakes are split by responsibility: `azure_fakes.py`, `dbutils_fake.py`, `stub.py`, `schedule.py`, `fixture.py`.
- `dbutils.fs` is sandboxed to `fs_root` (a destructive `rm` must not escape the tmp dir).
- `UseDevelopmentStorage=true` is confirmed to construct clients with the pinned SDKs (table `127.0.0.1:10002`, queue `127.0.0.1:10001`). Exception type for a missing-row transaction delete on Azurite is asserted as `HttpResponseError` (both `ResourceNotFoundError` and `TableTransactionError` derive from it).
- `create_queue` on an existing queue is a no-op in the fake (Azure Queue REST returns 204 when the metadata is identical; only differing metadata is a 409). The spec said it raises `ResourceExistsError`; the contract test settles which is right on Azurite.

## Execution status (updated 2026-09-30)

Branch `claude/github-issue-220-447fc0`; every commit below used `--no-verify` after manual `ruff format`/`ruff check` on the changed files and `uv run ty check fabricks/` (user-approved: the repo pre-commit hook fails at baseline on `ty check tests/` — 39 diagnostics in `tests/unit/plain/fixtures/notebooks/`, unrelated). The spec and this plan sit under gitignored `docs/superpowers/` (`docs/**`, `.gitignore:180`; CONSTITUTION §5 says new documentation paths are ignored on purpose); at the user's explicit request they were force-added (`git add -f`) in one docs commit on this branch. Drop that commit (or `git rm --cached` both files) before merging if the repo should keep them untracked.

| Task | Status | Commit(s) |
|---|---|---|
| 0 (spikes, spec/plan commit) | baseline recorded; spikes 2-4 deferred to Change 2; spec + plan force-added in the docs commit at the user's request | see `git log -- docs/superpowers` |
| 1 lazy `databricks.sdk.runtime` imports + seam guard | done, review clean | `b8c2e9a5` |
| 2 lazy `dags.log` table | done, review clean | `9749cc39` |
| 2b (added) `flush()` early return on empty buffer | done, review clean | `a1135d75` |
| 3 delete wholesale `dags.log`/runtime fakes | done after 2 fix rounds, review clean; **Apache edit unverified by execution** | `c195ec3c`, `e68a2ab7`, `63568b72` |
| 4 per-test reset + process-state finalizers | done, review clean (inline review 2026-09-30: config 205 / plain 131 green, ruff clean; nits: test name claims "assigned_children" but assigned children/plain attrs survive the reset, `_mock_children` is private API) | `c1f5d663` |
| 5 Spark Connect cache | done (config 206 green; ruff SIM300 needed `before == resolver._STEP_SESSIONS` operand order) | `0d3dcce9` |
| 6 env-reload fixture | done (plain 132, config 206 green); the plan's test was vacuous, so it restores to `"remote"` instead of `"databricks"` (an unset variable reloads to `"databricks"` by default); mutation-checked | `f9ad1014` |
| 7 `docs/TEST.md` + Change 1 verification | docs done; plain 132 / config 206 green; **Apache tier green: 88 passed** (incl. the Task 3-edited `test_silver_skip_unchanged.py`), run on Temurin JDK 21.0.12 via `JAVA_HOME=~/.local/jdk/jdk-21.0.12.1+1` (JDK 25 fails: pyspark 4.1.x bundles Hadoop 3.4.2, whose `Subject.getSubject` call JDK 23+ rejects; pyspark 4.2.0 is blocked by `sparkdantic`'s `<4.2.0` guard). `just lint` fails only on 3 pre-existing `ty` diagnostics in `fabricks/utils/spark.py` | `38269b23` |
| Docs (user request) | CONSTITUTION §2: import `databricks.sdk.runtime` inside functions | `0c377196` |
| 8 strict Spark mock | done, zero fallout (config 207 green) | see `git log` |
| 9 stub + dbutils fake | done (plain 141, config 207 green; file-level `noqa: N802,N803` because names mirror the camelCase SDK stub) | see `git log` |
| 10 Azure Table/Queue fakes | done (plain 156, config 207 green; lint-only edits to the plan code: PT018 split asserts, `match=` on ValueError, `_ = view.sent`) | see `git log` |
| 11 semblance fixture + schedule rows | done (plain 161, config 210 green, Apache collects 88). Deviation: `test_semblance_leak.py` overrides the autouse `_fake_dags_log_table` by name (like `test_dags_log_import.py`), else its "unresolved outside the fixture" test sees the conftest MagicMock. `_fake_dags_log_table` stays in the config conftest until Task 12 removes its last dependents | see `git log` |
| 12 migrate DagProcessor fakes | done with user approval (CONSTITUTION §4); config 211 green; all 7 old test names kept + 1 new. `_fake_dags_log_table` must stay: `test_dags_run_status.py` (7 tests) still needs it | see `git log` |
| 13 local schedule test | done (config 213 green) | see `git log` |
| 14 Azurite contract test | done; fake backend 5 pass, **Azurite 10/10 pass** (`npx azurite`, Node present). Contract found one disagreement: Azurite raises `ResourceExistsError` for `create_queue` on an existing queue (the fake no-op'd); fake + its test fixed, contract test now goes through `AzureQueue.create_if_not_exists()` (what Fabricks calls). plain 166 + 5 skipped, config 213 | see `git log` |
| 15 Apache uses semblance + docs + final verification | done. Final: plain 166 + 5 skipped (Azurite cases), config 213, **Apache 88 (JDK 21)**, Azurite contract 10/10. `just lint` red only on the pre-existing `ty` baseline (3 in `fabricks/utils/spark.py`, 39 in `tests/unit/plain/fixtures/notebooks/`); no new diagnostics | see `git log` |

Baseline now (config/plain only; Apache never run): plain 131 passed, config 205 passed.

**What changed versus this plan's text (read before continuing):**
- **Task 2:** the lazy-table discriminator is `isinstance(table, AzureTable)`, not `callable(table)` (`ty check fabricks/` rejects the callable narrowing). A duck-typed fake passed to the constructor is treated as a factory, so tests must patch `TABLE_LOG_HANDLER._table` (Task 11 already does).
- **Task 3:** deleting the config-tier `dags.log` fake broke 13 config tests. Final design: one autouse `_fake_dags_log_table` fixture in `tests/unit/config/conftest.py` that imports `TABLE_LOG_HANDLER` **inside its body** (a `sys.modules` guard was tried and rejected: it fails when a receive test file is run alone), sets `TABLE_LOG_HANDLER._table` to a `MagicMock`, and calls `clear_buffer()` on teardown. `test_dags_log_import.py` overrides it by name to see the untouched handler. The Apache test uses the same seam (`_fake_databricks_runtime` + `_fake_dags_log_table`).
- **Task 4:** `reset_mock(return_value=True, side_effect=True)` also resets magic-method defaults (`bool(SPARK)` returned a `MagicMock` and raised `TypeError`; 19 config tests broke). As landed, `_reset_bootstrap_mocks()` does a plain `reset_mock()` and then `_clear_configured_results(mock)`, which walks the private `Mock._mock_children` clearing `side_effect` and `return_value` on non-dunder children; `test_reset_keeps_magic_method_defaults` pins it. Magic methods a test configures deliberately, or attributes a test deletes, are not undone between tests. Task 8 (strict Spark) builds on this function.
- **Lint learnings for Change 2 code:** ruff PT018 wants `assert a and b` split into two asserts (Task 9's `stub.py` snippet is fixed below); `just test-plain` / `just test-config` take only a TARGET (drop `-v` from the commands in this plan); `ty check fabricks/` must stay clean, `ty check tests/` cannot be used as a gate (baseline red).
- **Task 11 interplay:** the `semblance` fixture sets `TABLE_LOG_HANDLER._table` to a real `AzureTable` on the fake service; the config tier's autouse `_fake_dags_log_table` already set it to a `MagicMock`. Autouse fixtures run before requested ones and both use the function-scoped `monkeypatch`, so semblance's value wins and teardown restores in LIFO order — verify with `test_semblance_leak.py`, and delete `_fake_dags_log_table` from the config conftest only once no config test still depends on it.
- **Task 12:** it deletes `_dag_processor_helpers.py` and rewrites two existing test files. `docs/CONSTITUTION.md` §4 forbids modifying or deleting an existing test without explicit user approval, so get that approval before starting Task 12.
- **Task 7 / final verification:** the Apache tier has not been run in this effort (the user stopped the run). Someone must run `just test-apache` once before merge; `tests/spark/apache/test_silver_skip_unchanged.py` was edited (Task 3) but never executed.
- Commit trailer: implementers were told to use `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`; commit `a1135d75` carries the Haiku 4.5 trailer of the model that wrote it.

## File Structure

**Change 1 (seams + restoration):**
- Modify `framework/fabricks/core/dags/run.py`, `framework/fabricks/core/dags/processor.py`, `framework/fabricks/core/schedules/dags.py` — lazy `databricks.sdk.runtime` imports (A2).
- Modify `framework/fabricks/utils/log.py` — `AzureTableLogHandler` lazy table (A1); `framework/fabricks/core/dags/log.py` passes the factory.
- Modify `framework/tests/unit/config/conftest.py` — delete `dags.log` fake, per-test reset, session finalizer.
- Modify `framework/tests/spark/apache/test_silver_skip_unchanged.py`, `framework/tests/unit/config/test_dags_run_status.py`, `test_build_step_spark_session_connect.py`, `framework/tests/unit/plain/test_local_file_share_path.py`.
- Create `framework/tests/unit/plain/test_seams.py`, `test_azure_table_log_handler_lazy.py`; `framework/tests/unit/config/test_dags_log_import.py`, `test_bootstrap_reset.py`.
- Modify `docs/TEST.md` (mocking-strategy paragraph).

**Change 2 (harness):**
- Create `framework/tests/semblance/{__init__,stub,dbutils_fake,azure_fakes,schedule,fixture}.py`.
- Modify `framework/tests/conftest.py` (`pytest_plugins`), `framework/tests/unit/config/conftest.py` (strict Spark, drop `no_real_sleep`).
- Create tests: `unit/plain/test_semblance_dbutils.py`, `test_semblance_azure.py`, `test_semblance_fixture.py`, `test_azure_contract.py`; `unit/config/test_semblance_leak.py`, `test_local_schedule.py`.
- Rewrite `unit/config/test_dag_receive_status.py`, `test_dag_receive_skips_unchanged.py`; delete `unit/config/_dag_processor_helpers.py`.
- Modify `tests/spark/apache/test_silver_skip_unchanged.py` (use `semblance`), `docs/TEST.md` (worked example + Azurite recipe).

---

# Change 1 — Seams and restoration fixes

### Task 0: Baseline, spike, and commit the spec and plan

**Files:**
- Create: (none; findings appended to the end of this plan)
- Modify: `docs/superpowers/specs/2026-09-29-local-test-runtime-harness-design.md` (sync deviations)

**Interfaces:** Produces the baseline pass counts and the strict-Spark fallout list that Task 8 consumes.

- [ ] **Step 1: Baseline all three local tiers**

`pytest` lives in the `test` dependency group, which a plain `uv sync` does not install: run `uv sync --group test` first. Then run (one per invocation):
```bash
just test-plain 2>&1 | tail -3
just test-config 2>&1 | tail -3
just test-apache 2>&1 | tail -3
```
Expected: all green. Record the pass counts in "Spike results" at the bottom of this file. If any tier is red before changes, stop and report.

- [ ] **Step 2: Spike — strict Spark fallout (temporary edit, do not commit)**

In `tests/unit/config/conftest.py` change `_fake_spark_session = MagicMock(name="fake_spark_session")` to
```python
from pyspark.sql import SparkSession as _RealSparkSession

_fake_spark_session = MagicMock(spec=_RealSparkSession, name="fake_spark_session")
```
Run `just test-config 2>&1 | tail -40`. Record every failing test and the missing attribute in "Spike results". Then `git checkout tests/unit/config/conftest.py`.

- [ ] **Step 3: Spike — `DagProcessor` builds in the config tier**

Create throwaway `tests/unit/config/test_spike_tmp.py`:
```python
from fabricks.core.dags.processor import DagProcessor


def test_processor_constructs():
    p = DagProcessor(schedule_id="s1", schedule="daily", step="silver", notebook=False)
    assert str(p.step) == "silver"
```
Run `just test-config tests/unit/config/test_spike_tmp.py -v`. Record pass/fail and the error. Delete the file.

- [ ] **Step 4: Spike — stub loads by path while `databricks.sdk.runtime` is faked**

Create throwaway `tests/unit/config/test_spike_tmp.py`:
```python
import importlib.util
from pathlib import Path


def test_stub_loads_by_path():
    spec = importlib.util.find_spec("databricks.sdk")
    path = Path(spec.submodule_search_locations[0]) / "runtime" / "dbutils_stub.py"
    s = importlib.util.spec_from_file_location("_stub", path)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    assert hasattr(m.dbutils.notebook, "run")
```
Run it in the config tier (where `databricks.sdk.runtime` is a `MagicMock`). Expected: PASS. Delete the file.

- [ ] **Step 5: Confirm the spec matches this plan's deviations** (already applied; verify, do not redo)

`grep -n "ResourceExistsError\|widgets.set\|setter" docs/superpowers/specs/*.md` should return nothing. The edits were: (a) handle example — replace `fakes.dbutils.widgets.set(...)`, `.secrets.set(...)`, `.taskValues.set(...)`, `.notebook.on_run(...)` with `semblance.widgets["schedule_id"] = "s1"`, `semblance.secrets[("scope", "key")] = "value"`, `semblance.task_values["schedule"] = "daily"`, `semblance.on_notebook_run(returns="success")`, and rename `fakes`/`boundary` wording to `semblance`; (b) "Step 0 A1": the fixture sets `TABLE_LOG_HANDLER._table` (no setter); (c) Components: file split (`azure_fakes.py`, `dbutils_fake.py`, `stub.py`, `schedule.py`, `fixture.py`) and `fs` sandboxed to `fs_root`; (d) Azure Queue fake: `create_queue` on an existing queue is a **no-op**, not `ResourceExistsError` (Azure Queue REST returns 204 when metadata is identical; the contract test confirms on Azurite); (e) contract/Testing: missing-row delete asserted as `HttpResponseError`; (f) Open items: mark `UseDevelopmentStorage=true` construction verified (table `127.0.0.1:10002`, queue `127.0.0.1:10001`).

- [ ] **Step 6: Commit spec and plan**

```bash
git add docs/superpowers/specs docs/superpowers/plans
git commit -m "docs: semblance test harness spec and implementation plan (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 1: A2 — lazy `databricks.sdk.runtime` imports and the seam guard

**Files:**
- Modify: `fabricks/core/dags/run.py:5,85-97`, `fabricks/core/dags/processor.py:8,157`, `fabricks/core/schedules/dags.py:1,12-126`
- Modify: `tests/unit/config/test_dags_run_status.py:10`
- Create: `tests/unit/plain/test_seams.py`

**Interfaces:** Produces: no module under `fabricks/` (except `fabricks/api/notebooks/`) has a module-level `databricks.sdk.runtime` import. The `test_seams.py` guard enforces it.

- [ ] **Step 1: Write the failing seam-guard test**

`tests/unit/plain/test_seams.py`:
```python
"""Seam guards: production modules must stay importable without a Databricks runtime."""

import ast
import importlib.util
from pathlib import Path

_NOTEBOOK_ENTRY_POINTS = "api/notebooks"  # Databricks notebook sources import dbutils at module level by design


def _fabricks_root() -> Path:
    spec = importlib.util.find_spec("fabricks")
    assert spec and spec.submodule_search_locations
    return Path(spec.submodule_search_locations[0])


def _module_level_nodes(node: ast.AST):
    """Yield statements that execute at import time (skip function bodies)."""
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            yield from _module_level_nodes(child)


def _imports_sdk_runtime(node: ast.AST) -> bool:
    if isinstance(node, ast.ImportFrom):
        return (node.module or "").startswith("databricks.sdk.runtime")
    if isinstance(node, ast.Import):
        return any(a.name.startswith("databricks.sdk.runtime") for a in node.names)
    return False


def test_no_module_level_databricks_sdk_runtime_import():
    root = _fabricks_root()
    offenders = []
    for path in root.rglob("*.py"):
        if _NOTEBOOK_ENTRY_POINTS in path.as_posix():
            continue
        tree = ast.parse(path.read_text())
        for node in _module_level_nodes(tree):
            if _imports_sdk_runtime(node):
                offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert not offenders, f"module-level databricks.sdk.runtime imports (move inside the function): {offenders}"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `just test-plain tests/unit/plain/test_seams.py -v`
Expected: FAIL listing `core/dags/processor.py:8`, `core/dags/run.py:5`, `core/schedules/dags.py:1`.

- [ ] **Step 3: Move the imports inside the functions**

`fabricks/core/dags/run.py`: delete line 5 (`from databricks.sdk.runtime import dbutils`). In `run()` add the import in each block that uses it:
```python
    if schedule_id is None:
        from databricks.sdk.runtime import dbutils

        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    if schedule is None:
        from databricks.sdk.runtime import dbutils

        try:
            schedule = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule = dbutils.widgets.get("schedule")

    if notebook_id is None:
        try:
            from databricks.sdk.runtime import dbutils

            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())  # type: ignore
            notebook_id = context.get("tags").get("jobId")
        except:  # noqa: E722
            notebook_id = None
```
(The `notebook_id` import stays inside its `try` so a failing runtime import is swallowed exactly as before.)

`fabricks/core/dags/processor.py`: delete line 8. In `receive()`, inside `if self.notebook:` add `from databricks.sdk.runtime import dbutils` as the first statement of that branch (above `path: str = ...`).

`fabricks/core/schedules/dags.py`: delete line 1. Add as the first statement of each function body that uses them:
- `standalone`: `from databricks.sdk.runtime import dbutils, spark`
- `terminate`, `process`, `generate`: `from databricks.sdk.runtime import dbutils`

Confirm nothing was missed: `grep -n "dbutils\|spark\." fabricks/core/schedules/dags.py fabricks/core/dags/run.py fabricks/core/dags/processor.py` — every use must be inside a function that imports it.

- [ ] **Step 4: Fix the test that patched the removed module attribute**

`tests/unit/config/test_dags_run_status.py`: replace the imports/helper head with
```python
import sys
from unittest.mock import MagicMock, patch

import pytest

from fabricks.core.dags.run import run
from fabricks.core.jobs.base.exception import CheckWarning, PreRunCheckException, SkipWarning, UnchangedWarning


def _run_with_mocked_dbutils(fake_job):
    # run() imports dbutils lazily from databricks.sdk.runtime (faked by this tier's conftest)
    with patch.object(sys.modules["databricks.sdk.runtime"], "dbutils") as fake_dbutils:
        fake_dbutils.jobs.taskValues.get.return_value = "sched-1"
        fake_dbutils.notebook.entry_point.getDbutils.side_effect = Exception("no notebook context in a unit test")
        return run(job=fake_job, schedule_id="sched-1", schedule="daily")
```
(keep the remaining tests unchanged).

- [ ] **Step 5: Run the tests**

```bash
just test-plain tests/unit/plain/test_seams.py -v
just test-config
```
Expected: seam test PASS; config tier green.

- [ ] **Step 6: Commit**

```bash
just format && just lint
git add fabricks tests
git commit -m "refactor(dags): import databricks.sdk.runtime lazily instead of at module level" \
  -m "Removes the import-time coupling that forced tests to fake sys.modules; adds an ast seam guard." \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 2: A1 — `dags/log.py` does no I/O at import

**Files:**
- Modify: `fabricks/utils/log.py` (imports, `AzureTableLogHandler.__init__`, `get_logger` signature), `fabricks/core/dags/log.py`
- Create: `tests/unit/plain/test_azure_table_log_handler_lazy.py`

**Interfaces:**
- Produces: `AzureTableLogHandler(table: AzureTable | Callable[[], AzureTable], ...)`; `.table` property resolves the factory once; private `._table` holds the resolved table (`None` until resolved) — Task 9's fixture patches it.
- Produces: `get_logger(..., table: AzureTable | Callable[[], AzureTable] | None = None, ...)`.

- [ ] **Step 1: Write the failing test**

`tests/unit/plain/test_azure_table_log_handler_lazy.py`:
```python
from unittest.mock import MagicMock

from fabricks.utils.azure_table import AzureTable
from fabricks.utils.log import AzureTableLogHandler


def test_factory_is_not_called_until_the_table_is_used():
    factory = MagicMock(return_value=MagicMock(name="table"))
    handler = AzureTableLogHandler(table=factory)

    factory.assert_not_called()
    assert handler.table is factory.return_value
    assert handler.table is factory.return_value
    factory.assert_called_once_with()


def test_a_table_instance_is_used_as_is():
    table = AzureTable("t", connection_string="UseDevelopmentStorage=true")  # constructing does no I/O
    handler = AzureTableLogHandler(table=table)

    assert handler.table is table
```

- [ ] **Step 2: Run to verify it fails**

Run: `just test-plain tests/unit/plain/test_azure_table_log_handler_lazy.py -v`
Expected: FAIL (`handler.table` is the factory itself; `factory.assert_not_called` passes but `is factory.return_value` fails).

- [ ] **Step 3: Implement the lazy table**

`fabricks/utils/log.py`: add `from collections.abc import Callable` (top, stdlib block). Replace `AzureTableLogHandler.__init__` and add the property:
```python
class AzureTableLogHandler(logging.Handler):
    def __init__(
        self,
        table: AzureTable | Callable[[], AzureTable],
        debugmode: bool | None = False,
        timezone: str | ZoneInfo | None = None,
    ) -> None:
        super().__init__()

        self.buffer = []
        # a factory defers storage-account and secret resolution until the first write, so importing
        # fabricks.core.dags.log does no I/O
        self._table_factory = table if callable(table) else None
        self._table: AzureTable | None = None if callable(table) else table

        self.debugmode = False if debugmode is None else debugmode
        self.timezone = ZoneInfo(timezone) if isinstance(timezone, str) else (timezone or UTC)

    @property
    def table(self) -> AzureTable:
        if self._table is None:
            assert self._table_factory is not None
            self._table = self._table_factory()
        return self._table
```
Change `get_logger`'s parameter to `table: AzureTable | Callable[[], AzureTable] | None = None`.

`fabricks/core/dags/log.py`:
```python
import logging
from typing import Final

from fabricks.core.dags.utils import get_table
from fabricks.utils.log import AzureTableLogHandler, get_logger

# get_table is passed as a factory: resolving the storage account at import would need a real
# Azure FileSharePath, which local runs (LocalFileSharePath) do not have
Logger, TableLogHandler = get_logger("dags", logging.INFO, table=get_table, debugmode=False)

LOGGER: Final[logging.Logger] = Logger
assert TableLogHandler is not None
TABLE_LOG_HANDLER: Final[AzureTableLogHandler] = TableLogHandler
```

- [ ] **Step 4: Run tests**

Run: `just test-plain tests/unit/plain/test_azure_table_log_handler_lazy.py -v` → PASS. Then `just test-plain` and `just test-config` → green (the config tier still fakes `dags.log`; that goes in Task 3).

- [ ] **Step 5: Commit**

```bash
just format && just lint
git add fabricks tests
git commit -m "refactor(dags): resolve the dags log table lazily instead of at import" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Delete the wholesale `dags.log` / runtime fakes

**Files:**
- Modify: `tests/unit/config/conftest.py:1-64 (docstring), 83-110`, `tests/spark/apache/test_silver_skip_unchanged.py:1-33`
- Create: `tests/unit/config/test_dags_log_import.py`

**Interfaces:** Consumes Task 1/2 (no import-time runtime binding; lazy log table). Produces: the real `fabricks.core.dags.log` is imported in the config and Apache tiers.

- [ ] **Step 1: Write the regression test**

`tests/unit/config/test_dags_log_import.py`:
```python
"""fabricks.core.dags.log must import for real (no fake) and resolve its table lazily."""

from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


def test_dags_log_is_the_real_module_and_resolves_nothing_at_import():
    assert type(LOGGER).__name__ == "Logger"
    assert type(TABLE_LOG_HANDLER).__name__ == "AzureTableLogHandler"
    assert TABLE_LOG_HANDLER._table is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `just test-config tests/unit/config/test_dags_log_import.py -v`
Expected: FAIL (`LOGGER` is a `MagicMock` from the conftest fake).

- [ ] **Step 3: Delete the config-tier `dags.log` fake and fix its docstring**

In `tests/unit/config/conftest.py` delete:
```python
sys.modules["fabricks.core.dags.log"] = MagicMock(
    name="fake_dags_log",
    LOGGER=MagicMock(name="fake_dags_logger"),
    TABLE_LOG_HANDLER=MagicMock(name="fake_table_log_handler"),
)
```
In the module docstring: change "Three independent real-Spark/Databricks-construction paths have to be defused, not one:" to "Two independent real-Spark construction paths have to be defused:", delete numbered items 3 and 4, and add after item 2:
```
`databricks.sdk.runtime` is still replaced in sys.modules below, as a tripwire: nothing imports it at
module level any more (Step 0 seam), but a stray real import would try to authenticate against a
workspace. Per-test runtime fakes come from the `semblance` fixture (tests/semblance/).
`fabricks.core.dags.log` is NOT faked: its table is resolved lazily, so it imports safely.
```

- [ ] **Step 4: Replace the Apache test's module-level fakes with a scoped fixture**

`tests/spark/apache/test_silver_skip_unchanged.py`: replace the module docstring paragraphs about faking and the two `if … not in sys.modules` blocks (lines ~10-33) with:
```python
"""End-to-end proof (real Spark/Delta) that ordinary Silver runs did not regress and that a genuinely
empty batch surfaces as a "stale" RunStatus through the real dags.run.run() wrapper.

run() reads dbutils lazily from databricks.sdk.runtime; outside Databricks that import tries to
authenticate, so the fixture below swaps in a fake for the duration of each test (auto-restored).
"""

import sys
from unittest.mock import MagicMock

import pytest

from fabricks.core import get_job
from fabricks.core.dags.run import run


@pytest.fixture(autouse=True)
def _fake_databricks_runtime(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", MagicMock(name="fake_databricks_sdk_runtime"))
```
Keep the test body unchanged.

- [ ] **Step 5: Run the tiers**

```bash
just test-config
just test-apache tests/spark/apache/test_silver_skip_unchanged.py -v
```
Expected: PASS. If a config test fails because it relied on the faked `dags.log` (e.g. a `MagicMock` `LOGGER`), fix that test to use `patch("fabricks.core.dags.<module>.LOGGER")` as the receive tests already do; do not restore the fake.

- [ ] **Step 6: Commit**

```bash
just format && just lint
git add tests
git commit -m "test: stop faking fabricks.core.dags.log wholesale" \
  -m "Real module now imports safely; the Apache runtime fake is scoped to one fixture (#220)." \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Restore process-wide state — conftest finalizers and per-test reset (spot 1)

**Files:**
- Modify: `tests/unit/config/conftest.py` (bottom + capture of originals), `tests/unit/plain/conftest.py`
- Create: `tests/unit/config/test_bootstrap_reset.py`

**Interfaces:** Produces `_reset_bootstrap_mocks()` (plain function in the config conftest) and the autouse fixture `_reset_bootstrap_mocks_fixture`.

- [ ] **Step 1: Write the failing test**

`tests/unit/config/test_bootstrap_reset.py`:
```python
"""The bootstrap Spark/dbutils mocks are shared by the whole process; they must be reset between tests."""

from fabricks.context import SPARK
from tests.unit.config import conftest as config_conftest


def test_reset_clears_configured_return_values_side_effects_and_assigned_children():
    SPARK.some_method.return_value = 42
    SPARK.other_method.side_effect = RuntimeError("leak")

    config_conftest._reset_bootstrap_mocks()

    assert SPARK.some_method.return_value != 42
    SPARK.other_method()  # would raise the leaked side effect


def test_reset_runs_automatically_around_every_test(request):
    assert "_reset_bootstrap_mocks_fixture" in request.fixturenames
```
Note: with the spec'd mock (Task 8) these attribute names must exist on `SparkSession`; Task 8 changes them to `sql` and `table`. For now the mock is permissive.

- [ ] **Step 2: Run to verify it fails**

Run: `just test-config tests/unit/config/test_bootstrap_reset.py -v`
Expected: FAIL (`_reset_bootstrap_mocks` does not exist).

- [ ] **Step 3: Rewrite the bootstrap section of the config conftest**

Keep the module docstring (already updated in Task 3). Replace everything from `import os` to the end of the file with the following. The originals are captured **before** the assignments they protect, and the `SparkSession.builder` original is read before it is replaced:
```python
import os
from pathlib import Path
import sys
from unittest.mock import MagicMock

from pyspark.sql import SparkSession
import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[3]

from tests.tier_policy import activate_tier  # noqa: E402

activate_tier("config")

# --- capture what the bootstrap below overwrites, so the session finalizer can restore it ---
_ENV_KEYS = (
    "FABRICKS_BASE",
    "FABRICKS_RUNTIME",
    "FABRICKS_CONFIG",
    "FABRICKS_ENVIRONMENT",
    "FABRICKS_IS_DEBUGMODE",
    "FABRICKS_IS_JOB_CONFIG_FROM_YAML",
)
_ORIGINAL_ENV = {key: os.environ.get(key) for key in _ENV_KEYS}
_REPLACED_MODULES = ("fabricks.utils.spark", "databricks.sdk.runtime")
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _REPLACED_MODULES}
_ORIGINAL_BUILDER = SparkSession.__dict__["builder"]

os.environ["FABRICKS_BASE"] = str(_FRAMEWORK_ROOT)
os.environ["FABRICKS_RUNTIME"] = "tests/spark/runtime"
os.environ["FABRICKS_CONFIG"] = "tests/spark/runtime/fabricks/conf.fabricks.yml"
os.environ["FABRICKS_ENVIRONMENT"] = "docker"
os.environ["FABRICKS_IS_DEBUGMODE"] = "FALSE"
os.environ["FABRICKS_IS_JOB_CONFIG_FROM_YAML"] = "TRUE"

_fake_spark_session = MagicMock(name="fake_spark_session")
_fake_dbutils = MagicMock(name="fake_dbutils")

sys.modules["fabricks.utils.spark"] = MagicMock(
    spark=_fake_spark_session,
    dbutils=_fake_dbutils,
    get_spark=MagicMock(return_value=_fake_spark_session),
    get_dbutils=MagicMock(return_value=_fake_dbutils),
)

sys.modules["databricks.sdk.runtime"] = MagicMock(
    name="fake_databricks_sdk_runtime", spark=_fake_spark_session, dbutils=_fake_dbutils
)


def _make_builder() -> MagicMock:
    builder = MagicMock(name="fake_spark_session_builder")
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.enableHiveSupport.return_value = builder
    builder.getOrCreate.return_value = _fake_spark_session
    return builder


SparkSession.builder = _make_builder()  # fabricks.context builds SPARK at import, before any fixture runs


def _reset_bootstrap_mocks() -> None:
    """The shared bootstrap mocks cannot be replaced per test (21 modules hold `SPARK` by value)."""
    _fake_spark_session.reset_mock(return_value=True, side_effect=True)
    _fake_dbutils.reset_mock(return_value=True, side_effect=True)


@pytest.fixture(autouse=True)
def _reset_bootstrap_mocks_fixture(monkeypatch):
    _reset_bootstrap_mocks()
    monkeypatch.setattr(SparkSession, "builder", _make_builder())
    yield
    _reset_bootstrap_mocks()


@pytest.fixture(scope="session", autouse=True)
def _restore_process_state():
    """Only matters when pytest runs more than once in a process (REPL, IDE runner, nested run): the
    runtests.py runners call pytest.main once, so this is otherwise a no-op. Kept so this conftest
    leaves no process-wide state behind."""
    yield
    SparkSession.builder = _ORIGINAL_BUILDER
    for name, original in _ORIGINAL_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original
    for key, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture
def no_real_sleep(monkeypatch):
    """Skip a real tenacity retry wait (e.g. invoker.py's wait_fixed(60)). Task 11 moves this into
    the shared semblance plugin."""
    import time

    monkeypatch.setattr(time, "sleep", lambda *_a, **_kw: None)
```
(The `no_real_sleep` fixture stays until Task 11 moves it.) `SparkSession.builder = _make_builder()` at import replaces the pyspark classproperty exactly as the old `SparkSession.builder = _fake_builder` did.

For `tests/unit/plain/conftest.py`, capture the originals before `mock_spark()` runs and add a session finalizer:
```python
_REPLACED_MODULES = ("fabricks.utils.spark", "fabricks.context", "fabricks.context.spark_session")
_ORIGINAL_MODULES = {name: sys.modules.get(name) for name in _REPLACED_MODULES}
```
(placed above the `mock_spark()` call), and after it:
```python
@pytest.fixture(scope="session", autouse=True)
def _restore_process_state():
    """See tests/unit/config/conftest.py: only matters for a process that runs pytest more than once."""
    yield
    for name, original in _ORIGINAL_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original
```

- [ ] **Step 4: Run tests**

Run: `just test-config` and `just test-plain`. Expected: green.

- [ ] **Step 5: Commit**

```bash
just format && just lint
git add tests
git commit -m "test: reset shared bootstrap mocks per test and restore process state at exit (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Spark Connect test restores the resolver cache (spot 2)

**Files:** Modify `tests/unit/config/test_build_step_spark_session_connect.py:31-39`

- [ ] **Step 1: Write the failing regression check** (append to the same file)

```python
def test_resolver_cache_is_left_unchanged_by_the_connect_test():
    before = dict(resolver._STEP_SESSIONS)
    test_build_step_spark_session_under_spark_connect(pytest.MonkeyPatch())
    assert resolver._STEP_SESSIONS == before
```
(add `import pytest` at the top). Using a bare `pytest.MonkeyPatch()` here means nothing undoes the test's own `setattr(resolver, "SPARK", …)`; wrap it:
```python
def test_resolver_cache_is_left_unchanged_by_the_connect_test():
    before = dict(resolver._STEP_SESSIONS)
    with pytest.MonkeyPatch.context() as mp:
        test_build_step_spark_session_under_spark_connect(mp)
    assert resolver._STEP_SESSIONS == before
```

- [ ] **Step 2: Run to verify it fails**

Run: `just test-config tests/unit/config/test_build_step_spark_session_connect.py -v`
Expected: FAIL — the connect test leaves `"connect_test_step"` in `_STEP_SESSIONS`.

- [ ] **Step 3: Fix the test**

In `test_build_step_spark_session_under_spark_connect` replace `resolver._STEP_SESSIONS.pop("connect_test_step", None)` with:
```python
    monkeypatch.setattr(resolver, "_STEP_SESSIONS", {})
```

- [ ] **Step 4: Run** `just test-config` → green.

- [ ] **Step 5: Commit** (`git add tests && git commit -m "test: restore the step-session cache after the Spark Connect test (#220)" -m "Co-Authored-By: …"`).

---

### Task 6: Env-reload fixture restores the environment before its final reload (spot 3)

**Files:** Modify `tests/unit/plain/test_local_file_share_path.py:53-68`

- [ ] **Step 1: Write the failing test** (append)

```python
def test_finalizer_reloads_modules_against_the_restored_environment(monkeypatch):
    from fabricks.utils import environment as environment_module
    from fabricks.utils.path import file_share as file_share_module

    monkeypatch.setenv("FABRICKS_ENVIRONMENT", "databricks")  # the value the fixture must restore to
    inner = pytest.MonkeyPatch()
    setter = _environment_setter(inner)
    try:
        next(setter)("docker")
        assert environment_module.FABRICKS_ENVIRONMENT == "docker"
        with pytest.raises(StopIteration):
            next(setter)
        assert environment_module.FABRICKS_ENVIRONMENT == "databricks"
    finally:
        monkeypatch.undo()
        importlib.reload(environment_module)
        importlib.reload(file_share_module)
```

- [ ] **Step 2: Run to verify it fails**

Run: `just test-plain tests/unit/plain/test_local_file_share_path.py -v`
Expected: FAIL (`_environment_setter` undefined; after refactor without the fix it would fail on the last assert).

- [ ] **Step 3: Refactor the fixture and fix the finalizer**

Replace the `set_environment` fixture with:
```python
def _environment_setter(monkeypatch):
    from fabricks.utils import environment as environment_module
    from fabricks.utils.path import file_share as file_share_module

    def _set(value: str):
        monkeypatch.setenv("FABRICKS_ENVIRONMENT", value)
        importlib.reload(environment_module)
        importlib.reload(file_share_module)
        return file_share_module

    yield _set

    # Restore the environment BEFORE the final reloads: monkeypatch's own teardown runs after this
    # finalizer, so reloading first would freeze the modules against a deleted variable.
    monkeypatch.undo()
    importlib.reload(environment_module)
    importlib.reload(file_share_module)


@pytest.fixture
def set_environment(monkeypatch):
    yield from _environment_setter(monkeypatch)
```

- [ ] **Step 4: Run** `just test-plain` → green.

- [ ] **Step 5: Commit** (`git add tests && git commit -m "test: reload after restoring FABRICKS_ENVIRONMENT in the env-reload fixture (#220)" -m "Co-Authored-By: …"`).

---

### Task 7: `docs/TEST.md` mocking strategy and Change 1 verification

**Files:** Modify `docs/TEST.md`

- [ ] **Step 1: Add the section** after the paragraph ending "…prove the same behavior.":

```markdown
## Mocking strategy

- Mock external boundaries only (Azure SDK clients, `dbutils`, the Spark session in the config
  tier); keep Fabricks' business behavior real.
- A fake must honor the parameters under test. Prefer a strict fake (fails on anything it does not
  model) over a permissive `MagicMock`.
- Spark semantics (merge, CDC, SQL results) are tested with real Spark and Delta in the Apache tier,
  never with a mocked session.
- Restore process-wide state. Use `monkeypatch` for `sys.modules`, `os.environ`, module attributes
  and caches; never assign them directly in a test. Anything a conftest must set at import time gets
  a finalizer.
- Per-test state is fresh or reset: the shared bootstrap mocks (`SPARK`, `DBUTILS`) are reset around
  every config-tier test.
- Tier-process isolation stays: run each tier in its own pytest invocation.
```

- [ ] **Step 2: Verify all three tiers**

```bash
just test-plain 2>&1 | tail -3
just test-config 2>&1 | tail -3
just test-apache 2>&1 | tail -3
just lint
```
Expected: all green; counts ≥ the Task 0 baseline plus the new tests.

- [ ] **Step 3: Commit**

```bash
git add docs/TEST.md
git commit -m "docs(test): document the mocking strategy and state-restoration rules (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

**Change 1 is complete here: open/merge it before starting Change 2.**

---

# Change 2 — The `semblance` harness

### Task 8: Strict Spark mock in the config tier

**Files:** Modify `tests/unit/config/conftest.py`, `tests/unit/config/test_bootstrap_reset.py`, plus any config test the spike flagged.

**Interfaces:** Produces: `_fake_spark_session` is `MagicMock(spec=SparkSession)`; `_make_builder()` returns a `MagicMock(spec=SparkSession.Builder)`.

- [ ] **Step 1: Write the failing test** (append to `tests/unit/config/test_bootstrap_reset.py`)

```python
import pytest


def test_spark_mock_rejects_attributes_the_real_session_does_not_have():
    with pytest.raises(AttributeError):
        SPARK.sqll("select 1")
    SPARK.sql("select 1")  # a real SparkSession method still works
```
Also change the first test to use real attribute names: `SPARK.sql.return_value = 42` / `SPARK.table.side_effect = RuntimeError("leak")` and the assertions accordingly (`SPARK.sql.return_value != 42`, `SPARK.table("t")`).

- [ ] **Step 2: Run to verify it fails**

Run: `just test-config tests/unit/config/test_bootstrap_reset.py -v` → the new test FAILS (`sqll` returns a mock).

- [ ] **Step 3: Make the mocks strict**

In `tests/unit/config/conftest.py` (`SparkSession` is already imported at the top after Task 4):
```python
_fake_spark_session = MagicMock(spec=SparkSession, name="fake_spark_session")
_fake_dbutils = MagicMock(name="fake_dbutils")
```
and in `_make_builder()` use `MagicMock(spec=SparkSession.Builder, name="fake_spark_session_builder")`.

- [ ] **Step 4: Run and fix fallout**

Run: `just test-config 2>&1 | tail -40`. For each failure from the Task 0 spike list: if the test touches an attribute that does not exist on `SparkSession` (e.g. `_jvm`), set it explicitly in that test (`monkeypatch.setattr(SPARK, "_jvm", MagicMock(), raising=False)`) — do not loosen the global mock. If the fixes exceed ~10 tests, stop, revert this task's conftest change, and split it into its own change (record in "Spike results").

- [ ] **Step 5: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(config): make the fake Spark session strict (spec=SparkSession) (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `stub.py` and the `dbutils` fake

**Files:**
- Create: `tests/semblance/__init__.py` (empty), `tests/semblance/stub.py`, `tests/semblance/dbutils_fake.py`
- Test: `tests/unit/plain/test_semblance_dbutils.py`

**Interfaces:**
- Produces: `conforms_to(path: str)` decorator (binds call args against the SDK stub signature; sets `.stub_path`); `stub_signature(path) -> inspect.Signature`.
- Produces: `DbutilsState(fs_root: Path)` with dicts `widgets: dict[str,str]`, `secrets: dict[tuple[str,str],str]`, `task_values: dict[str,Any]`, `notebook_calls: list[NotebookCall]`, `notebook_results: list[tuple[str|None, Any]]`.
- Produces: `FakeDbutils(state)` exposing `.widgets/.secrets/.fs/.notebook/.jobs.taskValues`; `NotebookCall(path, timeout_seconds, arguments)`; `NotebookExit` exception with `.value`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/plain/test_semblance_dbutils.py`:
```python
import inspect

import pytest

from tests.semblance.dbutils_fake import DbutilsState, FakeDbutils, NotebookCall, NotebookExit
from tests.semblance.stub import stub_signature


@pytest.fixture
def state(tmp_path):
    return DbutilsState(fs_root=tmp_path)


@pytest.fixture
def dbutils(state):
    return FakeDbutils(state)


def _public_callables(obj, prefix=""):
    for name in dir(obj):
        if name.startswith("_"):
            continue
        value = getattr(obj, name)
        path = f"{prefix}{name}"
        if callable(value):
            yield path, value
        else:
            yield from _public_callables(value, f"{path}.")


def test_every_fake_method_conforms_to_the_sdk_stub(dbutils):
    checked = dict(_public_callables(dbutils))
    assert checked, "expected the fake to expose methods"
    for path, fn in checked.items():
        assert getattr(fn, "stub_path", None) == path, f"{path} is not declared with @conforms_to"
        fake_params = list(inspect.signature(fn.__wrapped__).parameters)[1:]  # drop self
        assert fake_params == list(stub_signature(path).parameters), path


def test_a_wrong_argument_name_fails_at_the_fake(dbutils):
    with pytest.raises(TypeError):
        dbutils.widgets.get(nme="x")


def test_unknown_attribute_raises(dbutils):
    with pytest.raises(AttributeError):
        dbutils.credentials


def test_widgets_get_and_text(dbutils, state):
    with pytest.raises(ValueError):
        dbutils.widgets.get("schedule")
    dbutils.widgets.text("schedule", "daily")
    assert dbutils.widgets.get("schedule") == "daily"
    state.widgets["schedule"] = "hourly"
    dbutils.widgets.text("schedule", "daily")  # does not overwrite an existing value
    assert dbutils.widgets.get("schedule") == "hourly"


def test_secrets(dbutils, state):
    state.secrets[("scope-a", "k")] = "v"
    assert dbutils.secrets.get("scope-a", "k") == "v"
    assert [s.name for s in dbutils.secrets.listScopes()] == ["scope-a"]
    with pytest.raises(ValueError):
        dbutils.secrets.get("scope-a", "missing")


def test_task_values_raise_type_error_when_unset_like_real_dbutils(dbutils, state):
    with pytest.raises(TypeError):
        dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
    dbutils.jobs.taskValues.set(key="schedule_id", value="s1")
    assert dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id") == "s1"
    assert dbutils.jobs.taskValues.get(taskKey="initialize", key="nope", debugValue="d") == "d"


def test_fs_ls_rm_are_sandboxed_to_fs_root(dbutils, tmp_path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "sub").mkdir()
    listed = {i.name: i for i in dbutils.fs.ls(str(tmp_path))}
    assert set(listed) == {"a.txt", "sub/"}
    assert listed["sub/"].isDir() and not listed["a.txt"].isDir()

    assert dbutils.fs.rm(str(tmp_path / "sub"), recurse=True) is True
    assert not (tmp_path / "sub").exists()
    with pytest.raises(PermissionError):
        dbutils.fs.rm("/", recurse=True)
    with pytest.raises(FileNotFoundError):
        dbutils.fs.ls(str(tmp_path / "missing"))


def test_notebook_run_is_scripted_and_recorded(dbutils, state):
    with pytest.raises(AssertionError, match="unexpected dbutils.notebook.run"):
        dbutils.notebook.run("/n", 60, {})

    state.notebook_results.append((None, "success"))
    assert dbutils.notebook.run(path="/n", timeout_seconds=60, arguments={"a": "1"}) == "success"
    assert state.notebook_calls[-1] == NotebookCall("/n", 60, {"a": "1"})


def test_notebook_exit_raises_carrying_the_value(dbutils):
    with pytest.raises(NotebookExit) as exc:
        dbutils.notebook.exit("done")
    assert exc.value.value == "done"
```

- [ ] **Step 2: Run to verify it fails**

Run: `just test-plain tests/unit/plain/test_semblance_dbutils.py -v` → FAIL (modules missing).

- [ ] **Step 3: Implement `stub.py`**

```python
"""Bind fake `dbutils` calls against the SDK's real signatures.

The stub is loaded by file path, not `import databricks.sdk.runtime.dbutils_stub`: the config tier
replaces `databricks.sdk.runtime` in sys.modules with a non-package mock, and the real package's
__init__ tries to authenticate against a workspace. The stub is typing-only, so it can lag the real
runtime; the pinned databricks-sdk is the reference.
"""

import functools
import importlib.util
import inspect
from pathlib import Path
from types import ModuleType


@functools.cache
def _stub() -> ModuleType:
    spec = importlib.util.find_spec("databricks.sdk")
    assert spec
    assert spec.submodule_search_locations
    path = Path(spec.submodule_search_locations[0]) / "runtime" / "dbutils_stub.py"
    module_spec = importlib.util.spec_from_file_location("_semblance_dbutils_stub", path)
    assert module_spec and module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def stub_signature(path: str) -> inspect.Signature:
    """Signature of e.g. "widgets.get" or "jobs.taskValues.set" on the SDK's dbutils stub."""
    obj = _stub().dbutils
    for part in path.split("."):
        obj = getattr(obj, part)
    return inspect.signature(obj)


def conforms_to(path: str):
    """Decorate a fake method so calls are bound against the stub signature first (TypeError on mismatch)."""

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            stub_signature(path).bind(*args, **kwargs)
            return fn(self, *args, **kwargs)

        wrapper.stub_path = path
        return wrapper

    return decorate
```

- [ ] **Step 4: Implement `dbutils_fake.py`**

```python
"""Strict fake of the `dbutils` surface Fabricks calls. Anything not modelled raises AttributeError."""

from collections import namedtuple
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
import shutil
from typing import Any, NamedTuple

from tests.semblance.stub import conforms_to


class NotebookCall(NamedTuple):
    path: str
    timeout_seconds: int
    arguments: Mapping[str, str]


class NotebookExit(Exception):
    def __init__(self, value: str) -> None:
        super().__init__(value)
        self.value = value


class _FileInfo(namedtuple("_FileInfo", ["path", "name", "size", "modificationTime"])):
    def isDir(self) -> bool:  # the runtime's FileInfo has isDir(); the SDK stub's namedtuple does not
        return self.name.endswith("/")


_Scope = namedtuple("_Scope", ["name"])


@dataclass
class DbutilsState:
    fs_root: Path
    widgets: dict[str, str] = field(default_factory=dict)
    secrets: dict[tuple[str, str], str] = field(default_factory=dict)
    task_values: dict[str, Any] = field(default_factory=dict)
    notebook_calls: list[NotebookCall] = field(default_factory=list)
    # (path or None for any, returned status | callable(NotebookCall) -> status); latest registration wins
    notebook_results: list[tuple[str | None, Any]] = field(default_factory=list)


class _Widgets:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    @conforms_to("widgets.get")
    def get(self, name):
        if name not in self._values:
            raise ValueError(f"widget {name!r} is not defined")
        return self._values[name]

    @conforms_to("widgets.text")
    def text(self, name, defaultValue, label=None):
        self._values.setdefault(name, defaultValue)  # a value passed in by the job wins, as on Databricks


class _Secrets:
    def __init__(self, values: dict[tuple[str, str], str]) -> None:
        self._values = values

    @conforms_to("secrets.get")
    def get(self, scope, key):
        try:
            return self._values[(scope, key)]
        except KeyError:
            raise ValueError(f"secret {scope}/{key} not found") from None

    @conforms_to("secrets.listScopes")
    def listScopes(self):
        return [_Scope(name) for name in sorted({scope for scope, _ in self._values})]


class _Fs:
    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def _resolve(self, path: str) -> Path:
        p = Path(path).resolve()
        if not p.is_relative_to(self._root):
            raise PermissionError(f"{path!r} is outside the semblance fs_root {self._root}")
        return p

    def _info(self, p: Path) -> _FileInfo:
        suffix = "/" if p.is_dir() else ""
        stat = p.stat()
        return _FileInfo(f"{p}{suffix}", f"{p.name}{suffix}", stat.st_size, int(stat.st_mtime * 1000))

    @conforms_to("fs.ls")
    def ls(self, path):
        p = self._resolve(path)
        if not p.exists():
            raise FileNotFoundError(path)
        return [self._info(p)] if p.is_file() else [self._info(c) for c in sorted(p.iterdir())]

    @conforms_to("fs.rm")
    def rm(self, dir, recurse=False):
        p = self._resolve(dir)
        if not p.exists():
            return False
        if p.is_dir():
            if not recurse and any(p.iterdir()):
                raise OSError(f"{dir} is a non-empty directory; pass recurse=True")
            shutil.rmtree(p)
        else:
            p.unlink()
        return True


class _Notebook:
    def __init__(self, state: DbutilsState) -> None:
        self._state = state

    @conforms_to("notebook.run")
    def run(self, path, timeout_seconds, arguments):
        call = NotebookCall(path, timeout_seconds, dict(arguments))
        self._state.notebook_calls.append(call)
        for match, returns in reversed(self._state.notebook_results):
            if match is None or match == path:
                return returns(call) if isinstance(returns, Callable) else returns
        raise AssertionError(
            f"unexpected dbutils.notebook.run({path!r}, ...): register it with semblance.on_notebook_run()"
        )

    @conforms_to("notebook.exit")
    def exit(self, value):
        raise NotebookExit(value)


class _TaskValues:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    @conforms_to("jobs.taskValues.get")
    def get(self, taskKey, key, default=None, debugValue=None):
        # taskKey is ignored: a local run has a single task. Unset outside a job raises TypeError,
        # which is exactly what schedules/dags.py and dags/run.py catch before falling back to widgets.
        if key in self._values:
            return self._values[key]
        if debugValue is not None:
            return debugValue
        if default is not None:
            return default
        raise TypeError("Must pass debugValue when calling get outside of a job context. debugValue cannot be None.")

    @conforms_to("jobs.taskValues.set")
    def set(self, key, value):
        self._values[key] = value


class _Jobs:
    def __init__(self, values: dict[str, Any]) -> None:
        self.taskValues = _TaskValues(values)


class FakeDbutils:
    def __init__(self, state: DbutilsState) -> None:
        self.widgets = _Widgets(state.widgets)
        self.secrets = _Secrets(state.secrets)
        self.fs = _Fs(state.fs_root)
        self.notebook = _Notebook(state)
        self.jobs = _Jobs(state.task_values)
```

- [ ] **Step 5: Run to verify it passes**

Run: `just test-plain tests/unit/plain/test_semblance_dbutils.py -v` → PASS. If the conformance test flags a parameter-name mismatch, the fake's parameter names must equal the stub's (the stub is the authority).

- [ ] **Step 6: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(semblance): strict dbutils fake checked against the SDK stub (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Azure Table and Queue fakes

**Files:**
- Create: `tests/semblance/azure_fakes.py`
- Test: `tests/unit/plain/test_semblance_azure.py`

**Interfaces:**
- Produces: `TableStore`, `QueueStore` (plain state holders); `TableServiceFactory(store)` and `QueueClientFactory(store)` — callables standing in for the SDK classes (`__call__` + `from_connection_string`); `TableView(store, name)` with `.seed(rows)`, `.rows(**where) -> list[dict]`; `QueueView(store, name)` with `.create()`, `.send(content)`, `.sent`, `.pending`; `parse_filter(query) -> list[tuple[str, str]]`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/plain/test_semblance_azure.py`:
```python
from azure.core.exceptions import ResourceNotFoundError
import pytest

from tests.semblance.azure_fakes import (
    QueueClientFactory,
    QueueStore,
    QueueView,
    TableServiceFactory,
    TableStore,
    TableView,
    parse_filter,
)


@pytest.fixture
def tables():
    return TableStore()


@pytest.fixture
def table_client(tables):
    return TableServiceFactory(tables).from_connection_string("x").create_table_if_not_exists(table_name="t")


def test_parse_filter_grammar():
    assert parse_filter("") == []
    assert parse_filter("PartitionKey eq 'a' and JobId eq 'b'") == [("PartitionKey", "a"), ("JobId", "b")]
    assert parse_filter("Name eq 'it''s'") == [("Name", "it's")]


@pytest.mark.parametrize("bad", ["Rank gt 1", "A eq 1", "A eq 'x' or B eq 'y'", "not A eq 'x'"])
def test_parse_filter_rejects_anything_else(bad):
    with pytest.raises(NotImplementedError):
        parse_filter(bad)


def test_upsert_query_round_trip_sorted_by_partition_and_row_key(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "b", "RowKey": "2", "V": "x"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "a", "RowKey": "9", "V": "y"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "a", "RowKey": "1", "V": "z"})])

    rows = list(table_client.query_entities(""))
    assert [(r["PartitionKey"], r["RowKey"]) for r in rows] == [("a", "1"), ("a", "9"), ("b", "2")]
    assert [r["V"] for r in table_client.query_entities("PartitionKey eq 'a' and V eq 'y'")] == ["y"]


def test_upsert_merges_and_returned_rows_are_copies(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1", "A": "1", "B": "1"})])
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1", "B": "2"})])
    row = next(iter(table_client.query_entities("")))
    assert row["A"] == "1" and row["B"] == "2"

    row["A"] = "mutated"
    assert next(iter(table_client.query_entities("")))["A"] == "1"


def test_delete_of_a_missing_row_raises_and_the_transaction_is_atomic(table_client):
    table_client.submit_transaction([("upsert", {"PartitionKey": "p", "RowKey": "1"})])
    with pytest.raises(ResourceNotFoundError):
        table_client.submit_transaction(
            [("delete", {"PartitionKey": "p", "RowKey": "1"}), ("delete", {"PartitionKey": "p", "RowKey": "gone"})]
        )
    assert len(list(table_client.query_entities(""))) == 1


def test_a_transaction_must_share_one_partition(table_client):
    with pytest.raises(ValueError):
        table_client.submit_transaction(
            [("upsert", {"PartitionKey": "a", "RowKey": "1"}), ("upsert", {"PartitionKey": "b", "RowKey": "1"})]
        )


def test_unsupported_query_arguments_and_operations_fail_loudly(table_client):
    with pytest.raises(NotImplementedError):
        table_client.query_entities("", select=["A"])
    with pytest.raises(NotImplementedError):
        table_client.submit_transaction([("update", {"PartitionKey": "a", "RowKey": "1"})])


def test_state_is_shared_across_client_instances_and_drop_removes_the_table(tables):
    factory = TableServiceFactory(tables)
    factory(endpoint="e", credential=None).create_table_if_not_exists(table_name="t").submit_transaction(
        [("upsert", {"PartitionKey": "p", "RowKey": "1"})]
    )
    second = factory.from_connection_string("x").create_table_if_not_exists(table_name="t")
    assert len(list(second.query_entities(""))) == 1

    factory.from_connection_string("x").delete_table("t")
    with pytest.raises(ResourceNotFoundError):
        list(second.query_entities(""))


def test_table_view_seed_and_rows(tables):
    view = TableView(tables, "t")
    view.seed([{"PartitionKey": "p", "RowKey": "1", "S": "a"}, {"PartitionKey": "p", "RowKey": "2", "S": "b"}])
    assert [r["RowKey"] for r in view.rows(PartitionKey="p", S="b")] == ["2"]
    with pytest.raises(ResourceNotFoundError):
        TableView(tables, "typo").rows()
    with pytest.raises(TypeError):
        view.rows(Rank=1)


@pytest.fixture
def queues():
    return QueueStore()


def test_queue_requires_creation_and_creating_an_existing_queue_is_a_noop(queues):
    client = QueueClientFactory(queues).from_connection_string("x", queue_name="q")
    with pytest.raises(ResourceNotFoundError):
        client.send_message("a")
    client.create_queue()
    client.send_message("a")
    client.create_queue()  # Azure returns 204 for an existing queue with identical metadata
    assert QueueView(queues, "q").pending == ["a"]


def test_queue_is_fifo_and_receive_removes_but_sent_keeps_history(queues):
    client = QueueClientFactory(queues)(account_url="u", queue_name="q", credential=None)
    client.create_queue()
    assert client.receive_message() is None
    client.send_message("one")
    client.send_message("two")

    msg = client.receive_message()
    assert msg.content == "one"
    client.delete_message(msg)

    view = QueueView(queues, "q")
    assert view.pending == ["two"]
    assert view.sent == ["one", "two"]

    client.clear_messages()
    assert view.pending == [] and view.sent == ["one", "two"]
    client.delete_queue()
    with pytest.raises(ResourceNotFoundError):
        view.sent


def test_queue_only_supports_string_content(queues):
    client = QueueClientFactory(queues).from_connection_string("x", queue_name="q")
    client.create_queue()
    with pytest.raises(TypeError):
        client.send_message({"a": 1})
```

- [ ] **Step 2: Run to verify it fails** — `just test-plain tests/unit/plain/test_semblance_azure.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement `azure_fakes.py`**

```python
"""In-memory stand-ins for the Azure Table and Queue SDK clients Fabricks uses.

Models only what fabricks/utils/azure_table.py and azure_queue.py call. State lives in a store owned
by the caller, so it is fresh per test and shared by every client instance (AzureTable builds a new
TableServiceClient on each access when it has no connection string).
"""

from collections import deque
import copy
import re
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.queue import QueueMessage

_CLAUSE = re.compile(r"^(\w+) eq '((?:[^']|'')*)'$")


def parse_filter(query: str) -> list[tuple[str, str]]:
    """The only grammar Fabricks generates: `` or `Field eq 'v' and Field2 eq 'w'`.

    # ceiling: splits on " and ", so a value containing " and " is not supported.
    """
    query = query.strip()
    if not query:
        return []
    clauses = []
    for part in query.split(" and "):
        match = _CLAUSE.match(part.strip())
        if match is None:
            raise NotImplementedError(
                f"semblance supports only `Field eq 'value'` clauses joined by ' and ', got: {query!r}"
            )
        clauses.append((match.group(1), match.group(2).replace("''", "'")))
    return clauses


# ---- tables ----------------------------------------------------------------


class TableStore:
    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}


class FakeTableClient:
    def __init__(self, store: TableStore, name: str) -> None:
        self._store = store
        self.table_name = name

    def _rows(self) -> dict[tuple[str, str], dict[str, Any]]:
        try:
            return self._store.tables[self.table_name]
        except KeyError:
            raise ResourceNotFoundError(f"table {self.table_name!r} does not exist") from None

    def query_entities(self, query_filter: str = "", **kwargs: Any):
        if kwargs:
            raise NotImplementedError(f"unsupported query_entities arguments: {sorted(kwargs)}")
        clauses = parse_filter(query_filter)
        rows = self._rows()
        # sorted by (PartitionKey, RowKey) like the real service, not insertion order
        return iter([copy.deepcopy(row) for _key, row in sorted(rows.items()) if all(row.get(f) == v for f, v in clauses)])

    def submit_transaction(self, operations, **kwargs: Any):
        if kwargs:
            raise NotImplementedError(f"unsupported submit_transaction arguments: {sorted(kwargs)}")
        rows = self._rows()
        ops = list(operations)
        if len({entity["PartitionKey"] for _op, entity, *_ in ops}) > 1:
            raise ValueError("all entities in a transaction must share a PartitionKey")

        staged = {key: dict(row) for key, row in rows.items()}  # atomic: apply to a copy, commit at the end
        for op, entity, *_ in ops:
            key = (entity["PartitionKey"], entity["RowKey"])
            if op == "upsert":
                staged.setdefault(key, {}).update(copy.deepcopy(dict(entity)))  # merge, the SDK's default mode
            elif op == "delete":
                if key not in staged:
                    raise ResourceNotFoundError(f"entity {key} does not exist in table {self.table_name!r}")
                del staged[key]
            else:
                raise NotImplementedError(f"unsupported transaction operation {op!r}")
        rows.clear()
        rows.update(staged)
        return []


class FakeTableServiceClient:
    def __init__(self, store: TableStore) -> None:
        self._store = store

    def create_table_if_not_exists(self, table_name: str, **kwargs: Any) -> FakeTableClient:
        self._store.tables.setdefault(table_name, {})
        return FakeTableClient(self._store, table_name)

    def delete_table(self, table_name: str, **kwargs: Any) -> None:
        if table_name not in self._store.tables:
            raise ResourceNotFoundError(f"table {table_name!r} does not exist")
        del self._store.tables[table_name]

    def close(self) -> None:
        pass


class TableServiceFactory:
    """Stands in for the `TableServiceClient` class: callable plus `from_connection_string`."""

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def __call__(self, *args: Any, **kwargs: Any) -> FakeTableServiceClient:
        return FakeTableServiceClient(self.store)

    def from_connection_string(self, conn_str: str, **kwargs: Any) -> FakeTableServiceClient:
        return FakeTableServiceClient(self.store)


class TableView:
    """Test-side handle on one table."""

    def __init__(self, store: TableStore, name: str) -> None:
        self._store = store
        self._name = name

    def seed(self, rows: list[dict]) -> None:
        client = FakeTableServiceClient(self._store).create_table_if_not_exists(self._name)
        for row in rows:
            client.submit_transaction([("upsert", row)])

    def rows(self, **where: str) -> list[dict]:
        if self._name not in self._store.tables:
            raise ResourceNotFoundError(f"table {self._name!r} does not exist; tables: {sorted(self._store.tables)}")
        for field, value in where.items():
            if not isinstance(value, str):
                raise TypeError(f"rows() filters by string equality only; {field}={value!r} is not a str")
        query = " and ".join(f"{f} eq '{v.replace(chr(39), chr(39) * 2)}'" for f, v in where.items())
        return list(FakeTableClient(self._store, self._name).query_entities(query))


# ---- queues ----------------------------------------------------------------


class _FakeQueue:
    def __init__(self) -> None:
        self.pending: deque[str] = deque()
        self.sent: list[str] = []


class QueueStore:
    def __init__(self) -> None:
        self.queues: dict[str, _FakeQueue] = {}


class FakeQueueClient:
    # no visibility timeout / dequeue count: AzureQueue only ever receives then deletes
    def __init__(self, store: QueueStore, queue_name: str) -> None:
        self._store = store
        self.queue_name = queue_name

    def _queue(self) -> _FakeQueue:
        try:
            return self._store.queues[self.queue_name]
        except KeyError:
            raise ResourceNotFoundError(f"queue {self.queue_name!r} does not exist") from None

    def create_queue(self, **kwargs: Any) -> None:
        # idempotent: Azure returns 204 for an existing queue with identical metadata (metadata is not
        # modelled, so it is always identical) and 409 only when it differs
        self._store.queues.setdefault(self.queue_name, _FakeQueue())

    def send_message(self, content: str, **kwargs: Any) -> QueueMessage:
        if not isinstance(content, str):
            raise TypeError("semblance queue fake supports str content only")
        queue = self._queue()
        queue.pending.append(content)
        queue.sent.append(content)
        return QueueMessage(content=content)

    def receive_message(self, **kwargs: Any) -> QueueMessage | None:
        queue = self._queue()
        return QueueMessage(content=queue.pending.popleft()) if queue.pending else None

    def delete_message(self, message: Any, pop_receipt: str | None = None, **kwargs: Any) -> None:
        self._queue()  # the message was already removed on receive

    def clear_messages(self, **kwargs: Any) -> None:
        self._queue().pending.clear()

    def delete_queue(self, **kwargs: Any) -> None:
        self._queue()
        del self._store.queues[self.queue_name]

    def close(self) -> None:
        pass


class QueueClientFactory:
    """Stands in for the `QueueClient` class: callable plus `from_connection_string`."""

    def __init__(self, store: QueueStore) -> None:
        self.store = store

    def __call__(self, account_url: str | None = None, queue_name: str | None = None, credential: Any = None, **kwargs: Any) -> FakeQueueClient:
        assert queue_name, "queue_name is required"
        return FakeQueueClient(self.store, queue_name)

    def from_connection_string(self, conn_str: str, queue_name: str, **kwargs: Any) -> FakeQueueClient:
        return FakeQueueClient(self.store, queue_name)


class QueueView:
    """Test-side handle on one queue."""

    def __init__(self, store: QueueStore, name: str) -> None:
        self._store = store
        self._name = name

    def _queue(self) -> _FakeQueue:
        if self._name not in self._store.queues:
            raise ResourceNotFoundError(f"queue {self._name!r} does not exist; queues: {sorted(self._store.queues)}")
        return self._store.queues[self._name]

    def create(self) -> None:
        FakeQueueClient(self._store, self._name).create_queue()

    def send(self, content: str) -> None:
        FakeQueueClient(self._store, self._name).send_message(content)

    @property
    def sent(self) -> list[str]:
        """Every message ever sent, in order (receive deletes from `pending`, not from here)."""
        return list(self._queue().sent)

    @property
    def pending(self) -> list[str]:
        return list(self._queue().pending)
```

- [ ] **Step 4: Run to verify it passes** — `just test-plain tests/unit/plain/test_semblance_azure.py -v` → PASS.

- [ ] **Step 5: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(semblance): in-memory Azure Table and Queue fakes (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 11: The `semblance` fixture, handle, and schedule seed helpers

**Files:**
- Create: `tests/semblance/fixture.py`, `tests/semblance/schedule.py`
- Modify: `tests/conftest.py`, `tests/unit/config/conftest.py` (delete its `no_real_sleep`)
- Test: `tests/unit/plain/test_semblance_fixture.py`, `tests/unit/config/test_semblance_leak.py`

**Interfaces:**
- Consumes: Task 9 (`DbutilsState`, `FakeDbutils`), Task 10 (stores, factories, views), Task 2 (`TABLE_LOG_HANDLER._table`).
- Produces: fixture `semblance` returning `Semblance` with: `.dbutils: FakeDbutils`, `.widgets`, `.secrets`, `.task_values`, `.notebook_calls`, `.fs_root: Path`, `.table(name) -> TableView`, `.queue(name) -> QueueView`, `.on_notebook_run(returns, path=None)`.
- Produces: fixture `no_real_sleep` (shared; yields to other threads instead of sleeping).
- Produces: `status_row(job_id, *, status="scheduled", step="silver", job=None, rank=1, schedule_id="s1", schedule="daily") -> dict`; `dependency_row(job_id, parent_id, *, status="pending", step="silver", job=None, parent_step="silver", parent=None, schedule_id="s1", schedule="daily") -> dict`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/plain/test_semblance_fixture.py` (plain tier: no `databricks.sdk.runtime` pre-faked):
```python
import sys

from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable


def test_azure_wrappers_run_against_the_fakes(semblance):
    with AzureTable("t1", connection_string="UseDevelopmentStorage=true") as table:
        table.upsert([{"PartitionKey": "p", "RowKey": "1", "S": "a"}])
        assert table.query("PartitionKey eq 'p'")[0]["S"] == "a"
    assert semblance.table("t1").rows(PartitionKey="p")[0]["RowKey"] == "1"

    with AzureQueue("q1", connection_string="UseDevelopmentStorage=true") as queue:
        queue.create_if_not_exists()
        queue.create_if_not_exists()  # a second create is a no-op
        queue.send({"a": 1})
        assert queue.receive() == '{"a": 1}'
    assert semblance.queue("q1").sent == ['{"a": 1}']
    assert semblance.queue("q1").pending == []


def test_runtime_module_carries_the_fake_dbutils(semblance):
    from databricks.sdk.runtime import dbutils

    assert dbutils is semblance.dbutils
    semblance.widgets["schedule"] = "daily"
    assert sys.modules["databricks.sdk.runtime"].dbutils.widgets.get("schedule") == "daily"


def test_time_sleep_is_neutralised(semblance):
    import time

    start = time.monotonic()
    time.sleep(30)
    assert time.monotonic() - start < 1


def test_fixture_state_is_fresh_first(semblance):
    semblance.widgets["leak"] = "1"
    semblance.table("t").seed([{"PartitionKey": "p", "RowKey": "1"}])
    semblance.queue("q").create()
    semblance.queue("q").send("m")


def test_fixture_state_is_fresh_second(semblance):
    assert semblance.widgets == {}
    assert semblance.task_values == {}
    assert semblance.notebook_calls == []
    import pytest
    from azure.core.exceptions import ResourceNotFoundError

    with pytest.raises(ResourceNotFoundError):
        semblance.table("t").rows()
    with pytest.raises(ResourceNotFoundError):
        semblance.queue("q").sent
```
`tests/unit/config/test_semblance_leak.py` (config tier: bootstrap mocks and log handler):
```python
import sys

from fabricks.context import DBUTILS, SPARK
from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER


def test_first_test_dirties_everything(semblance):
    SPARK.sql.return_value = 42  # the shared bootstrap mocks: reset, not replaced, between tests
    DBUTILS.credentials.getServiceCredentialsProvider.return_value = "leak"
    LOGGER.info(
        "start",
        extra={
            "partition_key": "s1",
            "schedule_id": "s1",
            "schedule": "daily",
            "step": "silver",
            "job": "j",
            "target": "table",
        },
    )
    assert [r["Message"] for r in semblance.table("dags").rows(PartitionKey="s1")] == ["start"]


def test_second_test_sees_a_clean_slate(semblance):
    assert SPARK.sql.return_value != 42
    assert DBUTILS.credentials.getServiceCredentialsProvider.return_value != "leak"
    assert TABLE_LOG_HANDLER._table is not None  # patched for this test only
    assert "dags" not in semblance.tables.tables  # fresh store: no rows from the previous test


def test_handler_table_is_unresolved_outside_the_fixture():
    assert TABLE_LOG_HANDLER._table is None
```
(`semblance.tables` is the `TableStore`, a public attribute of the handle. The `DBUTILS` assertion relies on `fabricks.context.DBUTILS` being the config conftest's `_fake_dbutils`, since `get_dbutils` is mocked to return it.)

- [ ] **Step 2: Run to verify it fails** — `just test-plain tests/unit/plain/test_semblance_fixture.py -v` → FAIL (`fixture 'semblance' not found`).

- [ ] **Step 3: Implement `schedule.py`**

```python
"""Row builders for the dependency and status partitions DagGenerator writes (generator.py)."""


def status_row(
    job_id: str,
    *,
    status: str = "scheduled",
    step: str = "silver",
    job: str | None = None,
    rank: int = 1,
    schedule_id: str = "s1",
    schedule: str = "daily",
) -> dict:
    return {
        "PartitionKey": "statuses",
        "RowKey": job_id,
        "ScheduleId": schedule_id,
        "Schedule": schedule,
        "Step": step,
        "JobId": job_id,
        "Job": job or job_id,
        "Status": status,
        "Rank": rank,
    }


def dependency_row(
    job_id: str,
    parent_id: str,
    *,
    status: str = "pending",
    step: str = "silver",
    job: str | None = None,
    parent_step: str = "silver",
    parent: str | None = None,
    schedule_id: str = "s1",
    schedule: str = "daily",
) -> dict:
    dependency_id = f"{job_id}:{parent_id}"
    return {
        "PartitionKey": "dependencies",
        "RowKey": dependency_id,
        "DependencyId": dependency_id,
        "ScheduleId": schedule_id,
        "Schedule": schedule,
        "Step": step,
        "Job": job or job_id,
        "JobId": job_id,
        "ParentStep": parent_step,
        "Parent": parent or parent_id,
        "ParentId": parent_id,
        "Status": status,
    }
```

- [ ] **Step 4: Implement `fixture.py`**

```python
"""pytest plugin exposing `semblance`: fresh, strict fakes for the Azure and Databricks boundary.

Loaded via `pytest_plugins` in tests/conftest.py at collection time -- before the tier conftests
bootstrap the fake fabricks environment -- so nothing here may import fabricks at module level.
"""

import importlib
from pathlib import Path
import sys
import time
import types
from typing import Any

import pytest

from tests.semblance.azure_fakes import (
    QueueClientFactory,
    QueueStore,
    QueueView,
    TableServiceFactory,
    TableStore,
    TableView,
)
from tests.semblance.dbutils_fake import DbutilsState, FakeDbutils

_DEV_ACCOUNT = "semblance"


class Semblance:
    """What a test sees. Seed through the dicts and views, assert through the views."""

    def __init__(self, fs_root: Path) -> None:
        self._state = DbutilsState(fs_root=fs_root)
        self.dbutils = FakeDbutils(self._state)
        self.tables = TableStore()
        self.queues = QueueStore()

    @property
    def widgets(self) -> dict[str, str]:
        return self._state.widgets

    @property
    def secrets(self) -> dict[tuple[str, str], str]:
        return self._state.secrets

    @property
    def task_values(self) -> dict[str, Any]:
        return self._state.task_values

    @property
    def notebook_calls(self):
        return self._state.notebook_calls

    @property
    def fs_root(self) -> Path:
        return self._state.fs_root

    def table(self, name: str) -> TableView:
        return TableView(self.tables, name)

    def queue(self, name: str) -> QueueView:
        return QueueView(self.queues, name)

    def on_notebook_run(self, returns: Any, path: str | None = None) -> None:
        """Script dbutils.notebook.run: a status string, or a callable(NotebookCall) -> status. An
        unregistered run raises AssertionError."""
        self._state.notebook_results.append((path, returns))


@pytest.fixture
def no_real_sleep(monkeypatch):
    """Skip real waits (tenacity backoff, DagGenerator's time.sleep(60)); yield to other threads."""
    real_sleep = time.sleep
    monkeypatch.setattr(time, "sleep", lambda *_a, **_kw: real_sleep(0))


@pytest.fixture
def semblance(monkeypatch, tmp_path, no_real_sleep):
    s = Semblance(tmp_path)

    # Azure SDK boundary
    monkeypatch.setattr("fabricks.utils.azure_table.TableServiceClient", TableServiceFactory(s.tables))
    monkeypatch.setattr("fabricks.utils.azure_queue.QueueClient", QueueClientFactory(s.queues))

    # databricks.sdk.runtime: always a fresh module. Only the config tier pre-fakes it; the plain
    # tier does not, and importing the real one tries to authenticate against a workspace.
    fabricks_spark = sys.modules.get("fabricks.utils.spark") or importlib.import_module("fabricks.utils.spark")
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = s.dbutils  # type: ignore[attr-defined]
    runtime.spark = fabricks_spark.spark  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", runtime)
    monkeypatch.setattr(fabricks_spark, "dbutils", s.dbutils, raising=False)

    # Only when the test already imported the DAG modules (import them at module top, as tests do).
    base = sys.modules.get("fabricks.core.dags.base")
    if base is not None:
        connection = {"storage_account": _DEV_ACCOUNT, "access_key": "semblance-key", "credential": None}
        monkeypatch.setattr(base.BaseDags, "get_connection_info", lambda self: connection)

    log = sys.modules.get("fabricks.core.dags.log")
    if log is not None:
        from fabricks.utils.azure_table import AzureTable

        table = AzureTable("dags", storage_account=_DEV_ACCOUNT, access_key="semblance-key")
        monkeypatch.setattr(log.TABLE_LOG_HANDLER, "_table", table)

    return s
```

- [ ] **Step 5: Register the plugin and remove the old `no_real_sleep`**

`tests/conftest.py`:
```python
"""Shared pytest policy; tier-specific bootstrap stays in child conftests."""

from tests.tier_policy import mark_collected_tests

pytest_plugins = ["tests.semblance.fixture"]


def pytest_collection_modifyitems(items):
    mark_collected_tests(items)
```
Delete the `no_real_sleep` fixture (and the now-unused `import time`) from `tests/unit/config/conftest.py`. Verify nothing else defines it: `grep -rn "def no_real_sleep" tests`.

- [ ] **Step 6: Run to verify it passes**

```bash
just test-plain tests/unit/plain/test_semblance_fixture.py -v
just test-config tests/unit/config/test_semblance_leak.py tests/unit/config/test_invoker_transient_retry.py tests/unit/config/test_job_run_transient_retry.py -v
```
Expected: PASS. If `pytest_plugins` errors with "Defining 'pytest_plugins' in a non-top-level conftest", run from `framework/` with the tier path as the argument (`just test-*` does); if it still fails, move the registration to `pytest.ini`/`pyproject.toml` `addopts = -p tests.semblance.fixture` and record it in the spike results.

- [ ] **Step 7: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(semblance): function-scoped fixture, handle, and schedule row builders (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Migrate the hand-rolled `DagProcessor` fakes

**Files:**
- Rewrite: `tests/unit/config/test_dag_receive_status.py`, `tests/unit/config/test_dag_receive_skips_unchanged.py`
- Delete: `tests/unit/config/_dag_processor_helpers.py`

**Interfaces:** Consumes `semblance`, `status_row`, `dependency_row`.

- [ ] **Step 1: Rewrite `test_dag_receive_status.py`**

```python
import json
from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
QUEUE = f"qsilver{SCHEDULE_ID}"


def _receive(semblance, *, run_result=None, run_raises=None):
    job = status_row("job-1", status="waiting", job="silver.fact_dummy", schedule_id=SCHEDULE_ID)
    semblance.table(TABLE).seed(
        [
            job,
            dependency_row("child-1", "job-1", status="pending", schedule_id=SCHEDULE_ID),  # outgoing edge
            dependency_row("job-1", "parent-1", status="ok", schedule_id=SCHEDULE_ID),  # incoming edge
        ]
    )
    semblance.queue(QUEUE).create()
    semblance.queue(QUEUE).send(json.dumps(job))
    semblance.queue(QUEUE).send("SENTINEL")

    fake_job = MagicMock()
    fake_job.skip_if_stale = False
    with (
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run") as fake_run,
    ):
        if run_raises:
            fake_run.side_effect = run_raises
        else:
            fake_run.return_value = run_result
        with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
            processor.receive()


def _job_status(semblance) -> str:
    return semblance.table(TABLE).rows(PartitionKey="statuses", JobId="job-1")[0]["Status"]


def _outgoing_edge(semblance) -> dict:
    return semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="child-1")[0]


def test_receive_records_stale_status_on_a_real_exception_and_propagates_to_dependency_edges(semblance):
    _receive(semblance, run_raises=Exception("boom"))

    assert _job_status(semblance) == "stale"
    assert _outgoing_edge(semblance)["Status"] == "stale"


def test_receive_records_stale_status_and_updates_dependency_edges_not_delete(semblance):
    _receive(semblance, run_result="stale")

    assert _job_status(semblance) == "stale"
    # the outgoing edge this job just wrote must be updated in place, not deleted
    assert _outgoing_edge(semblance)["Status"] == "stale"


def test_receive_records_ok_status_on_a_successful_run_and_propagates_to_dependency_edges(semblance):
    # Control test, paired with the "stale" ones above: without it, a bug that always writes "stale"
    # regardless of the real outcome would pass every other test in this file.
    _receive(semblance, run_result="ok")

    assert _job_status(semblance) == "ok"
    assert _outgoing_edge(semblance)["Status"] == "ok"


def test_receive_deletes_its_own_incoming_edges(semblance):
    _receive(semblance, run_result="ok")

    assert semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="job-1") == []
```

- [ ] **Step 2: Rewrite `test_dag_receive_skips_unchanged.py`**

```python
import json
from unittest.mock import MagicMock, patch

from fabricks.core.dags.processor import DagProcessor
from fabricks.utils.log import LogStatus
from tests.semblance.schedule import dependency_row, status_row

SCHEDULE_ID = "sched-1"
TABLE = f"t{SCHEDULE_ID}"
QUEUE = f"qsilver{SCHEDULE_ID}"


def _receive(semblance, incoming_status: str | None, *, skip_if_stale: bool, run_result="ok", patch_logger=False):
    job = status_row("job-1", status="waiting", job="silver.fact_dummy", schedule_id=SCHEDULE_ID)
    rows = [job]
    if incoming_status is not None:
        rows.append(dependency_row("job-1", "parent-1", status=incoming_status, schedule_id=SCHEDULE_ID))
    semblance.table(TABLE).seed(rows)
    semblance.queue(QUEUE).create()
    semblance.queue(QUEUE).send(json.dumps(job))
    semblance.queue(QUEUE).send("SENTINEL")

    fake_job = MagicMock()
    fake_job.skip_if_stale = skip_if_stale
    patches = [
        patch("fabricks.core.dags.processor.get_job", return_value=fake_job),
        patch("fabricks.core.dags.processor.run", return_value=run_result),
    ]
    if patch_logger:
        patches.append(patch("fabricks.core.dags.processor.LOGGER"))

    mocks = [p.start() for p in patches]
    try:
        with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
            processor.receive()
    finally:
        for p in patches:
            p.stop()
    return mocks  # [get_job, run, (LOGGER)]


def test_receive_skips_dispatch_when_every_dependency_is_not_ok(semblance):
    _get_job, fake_run, fake_logger = _receive(semblance, "stale", skip_if_stale=True, patch_logger=True)

    fake_run.assert_not_called()
    logged = [call.args[0] for call in fake_logger.info.call_args_list]
    assert LogStatus.DONE in logged
    assert LogStatus.STALE in logged


def test_receive_dispatches_when_any_dependency_is_ok(semblance):
    _get_job, fake_run = _receive(semblance, "ok", skip_if_stale=True)

    fake_run.assert_called_once()


def test_receive_dispatches_when_there_are_no_dependencies_at_all(semblance):
    # The vacuous-truth guard: any(...) over an empty list is False, so `not any(...)` on zero
    # incoming edges is vacuously True -- without the explicit `and incoming` guard, a root job
    # would be treated as "none of my dependencies are ok" and skipped forever.
    _get_job, fake_run = _receive(semblance, None, skip_if_stale=True)

    fake_run.assert_called_once()


def test_receive_deletes_its_own_incoming_edges_after_reading_them_even_when_not_skipped(semblance):
    # skip_if_stale False never triggers a skip decision but must still delete incoming edges,
    # otherwise the 'dependencies' partition only ever grows.
    _receive(semblance, "ok", skip_if_stale=False)

    assert semblance.table(TABLE).rows(PartitionKey="dependencies", JobId="job-1") == []
```
`_receive` returns a list whose length depends on `patch_logger`; the unpacking in each test matches (3 mocks with logger, 2 without).

- [ ] **Step 3: Delete the helper and run**

```bash
git rm tests/unit/config/_dag_processor_helpers.py
grep -rn "_dag_processor_helpers\|fake_processor" tests   # expect no output
just test-config tests/unit/config/test_dag_receive_status.py tests/unit/config/test_dag_receive_skips_unchanged.py -v
```
Expected: PASS. If `DagProcessor(...)` fails to construct in the config tier, use the Task 0 spike result: the fix is another seam (resolve step/connection info lazily), not `__new__`; stop and report before working around.

- [ ] **Step 4: Run the full config tier** — `just test-config` → green.

- [ ] **Step 5: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(config): run DagProcessor.receive against semblance instead of MagicMock queue/table (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Local schedule test

**Files:** Create `tests/unit/config/test_local_schedule.py`

**Interfaces:** Consumes `semblance`, `status_row`.

- [ ] **Step 1: Write the test**

```python
"""Local schedule tests: real DagProcessor / logging against semblance's fake Azure -- no live service."""

import json

from fabricks.core.dags.log import LOGGER
from fabricks.core.dags.processor import DagProcessor
from tests.semblance.schedule import status_row

SCHEDULE_ID = "s1"


def test_send_dispatches_a_job_without_pending_dependencies_then_a_sentinel_per_worker(semblance):
    semblance.queue(f"qsilver{SCHEDULE_ID}").create()
    semblance.table(f"t{SCHEDULE_ID}").seed([status_row("job-1", job="silver.fact_dummy", rank=1)])

    with DagProcessor(schedule_id=SCHEDULE_ID, schedule="daily", step="silver", notebook=False) as processor:
        processor.send()
        workers = processor.step.workers

    sent = semblance.queue(f"qsilver{SCHEDULE_ID}").sent
    dispatched = json.loads(sent[0])
    assert dispatched["JobId"] == "job-1"
    assert dispatched["Status"] == "waiting"
    assert sent[1:] == ["SENTINEL"] * workers
    assert semblance.table(f"t{SCHEDULE_ID}").rows(PartitionKey="statuses", JobId="job-1")[0]["Status"] == "waiting"


def test_log_records_written_with_target_table_reach_the_dags_table(semblance):
    LOGGER.info(
        "start",
        extra={
            "partition_key": SCHEDULE_ID,
            "schedule_id": SCHEDULE_ID,
            "schedule": "daily",
            "step": "silver",
            "job": "silver.fact_dummy",
            "target": "table",
        },
    )

    rows = semblance.table("dags").rows(PartitionKey=SCHEDULE_ID)
    assert [r["Message"] for r in rows] == ["start"]
    assert rows[0]["Job"] == "silver.fact_dummy"
```

- [ ] **Step 2: Run** — `just test-config tests/unit/config/test_local_schedule.py -v` → PASS. If the `send()` test hangs, it is spinning on `get_scheduled`: check that the seeded row has `Step == "silver"` and `Status == "scheduled"`, and that `str(processor.step) == "silver"`.

- [ ] **Step 3: Commit**

```bash
just format && just lint
git add tests
git commit -m "test(config): local schedule tests for dispatch and log rows without live Azure (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Azurite contract test

**Files:** Create `tests/unit/plain/test_azure_contract.py`

**Interfaces:** Consumes `semblance` and the real `AzureTable`/`AzureQueue` wrappers.

- [ ] **Step 1: Write the contract test**

```python
"""The same scenarios through the real AzureTable/AzureQueue wrappers against the semblance fakes and,
opt-in, a real Azurite emulator, so the fakes cannot drift from Azure unnoticed.

Run Azurite yourself (no Docker):   npx azurite --silent --location "$(mktemp -d)"
then:  FABRICKS_TEST_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true" just test-plain tests/unit/plain/test_azure_contract.py
"""

import os
import uuid

from azure.core.exceptions import HttpResponseError
import pytest

from fabricks.utils.azure_queue import AzureQueue
from fabricks.utils.azure_table import AzureTable

_AZURITE = os.environ.get("FABRICKS_TEST_AZURITE_CONNECTION_STRING")


@pytest.fixture(
    params=[
        "fake",
        pytest.param("azurite", marks=pytest.mark.skipif(not _AZURITE, reason="set FABRICKS_TEST_AZURITE_CONNECTION_STRING")),
    ]
)
def backend(request):
    if request.param == "fake":
        request.getfixturevalue("semblance")
        return "UseDevelopmentStorage=true"
    return _AZURITE


@pytest.fixture
def table(backend):
    name = f"t{uuid.uuid4().hex[:12]}"
    with AzureTable(name, connection_string=backend) as t:
        t.create_if_not_exists()
        yield t
        t.drop()


@pytest.fixture
def queue(backend):
    name = f"q{uuid.uuid4().hex[:12]}"
    with AzureQueue(name, connection_string=backend) as q:
        q.create_if_not_exists()
        yield q
        q.delete()


def test_upsert_query_round_trip_and_filters(table):
    table.upsert(
        [
            {"PartitionKey": "b", "RowKey": "2", "Status": "pending", "JobId": "j2"},
            {"PartitionKey": "a", "RowKey": "1", "Status": "pending", "JobId": "j1"},
            {"PartitionKey": "a", "RowKey": "2", "Status": "ok", "JobId": "j1"},
        ]
    )

    assert [(r["PartitionKey"], r["RowKey"]) for r in table.query("")] == [("a", "1"), ("a", "2"), ("b", "2")]
    assert [r["RowKey"] for r in table.query("PartitionKey eq 'a' and JobId eq 'j1' and Status eq 'ok'")] == ["2"]


def test_upsert_merges_properties(table):
    table.upsert({"PartitionKey": "p", "RowKey": "1", "A": "1", "B": "1"})
    table.upsert({"PartitionKey": "p", "RowKey": "1", "B": "2"})

    row = table.query("PartitionKey eq 'p'")[0]
    assert (row["A"], row["B"]) == ("1", "2")


def test_deleting_a_missing_row_raises_an_azure_http_error(table):
    with pytest.raises(HttpResponseError):
        table.delete({"PartitionKey": "p", "RowKey": "never-existed"})


def test_creating_an_existing_queue_is_idempotent_and_keeps_its_messages(queue):
    queue.send("kept")

    queue.queue_client.create_queue()  # Azure returns 204 for an existing queue with identical metadata

    assert queue.receive() == "kept"


def test_queue_delivers_each_message_exactly_once(queue):
    for m in ("one", "two", "three"):
        queue.send(m)

    received = [queue.receive(), queue.receive(), queue.receive()]

    assert sorted(received) == ["one", "three", "two"]
    assert queue.receive() is None
```
Strict FIFO is deliberately not asserted here: real Azure queues do not guarantee it; the fake's FIFO is covered in `test_semblance_azure.py`.

- [ ] **Step 2: Run the fake backend** — `just test-plain tests/unit/plain/test_azure_contract.py -v` → fake cases PASS, azurite cases SKIPPED.

- [ ] **Step 3 (optional, needs Node): run against Azurite**

```bash
npx azurite --silent --location "$(mktemp -d)" &
FABRICKS_TEST_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true" just test-plain tests/unit/plain/test_azure_contract.py -v
```
Every failing azurite case is a place the fake disagrees with Azure: fix the fake (and its `test_semblance_azure.py` test), not the contract test. Record the outcome (or "not run: no Node") in "Spike results".

- [ ] **Step 4: Commit**

```bash
just format && just lint
git add tests
git commit -m "test: opt-in Azurite contract test that keeps the semblance fakes honest (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

### Task 15: Apache tier uses `semblance`; docs; final verification

**Files:** Modify `tests/spark/apache/test_silver_skip_unchanged.py`, `docs/TEST.md`

- [ ] **Step 1: Swap the Apache runtime fixture for `semblance`**

In `tests/spark/apache/test_silver_skip_unchanged.py` delete the `_fake_databricks_runtime` fixture and its imports (`sys`, `MagicMock`, `pytest` if unused) and request the fixture per test: `def test_dags_run_returns_stale_for_a_genuinely_empty_silver_batch(local_spark, monkeypatch, semblance):`. Update the module docstring to say the runtime fake comes from `semblance`.

Run: `just test-apache tests/spark/apache/test_silver_skip_unchanged.py -v` → PASS.

- [ ] **Step 2: Document the harness in `docs/TEST.md`**

Add under "Mocking strategy":
````markdown
### Writing a local test with `semblance`

Request the `semblance` fixture; it patches the Azure SDK clients and `databricks.sdk.runtime`, points
the DAG log table at an in-memory table, and makes `time.sleep` instant. Import `DagProcessor` (and
anything else from `fabricks.core.dags`) at the top of the test module so the fixture can see it.

```python
def test_dispatch(semblance):
    semblance.widgets["schedule_id"] = "s1"                      # dbutils.widgets.get
    semblance.secrets[("scope", "key")] = "value"                # dbutils.secrets.get
    semblance.task_values["schedule"] = "daily"                  # dbutils.jobs.taskValues
    semblance.on_notebook_run(returns="ok")                      # scripts dbutils.notebook.run
    semblance.queue("qsilvers1").create()
    semblance.table("ts1").seed([status_row("job-1")])          # from tests.semblance.schedule

    ...  # drive real Fabricks code

    semblance.table("ts1").rows(PartitionKey="statuses", Status="waiting")   # list[dict]
    semblance.queue("qsilvers1").sent      # every message ever sent, in order
    semblance.queue("qsilvers1").pending   # not yet received
    semblance.notebook_calls               # [NotebookCall(path, timeout_seconds, arguments)]
```

The fakes are strict: an unsupported filter, a missing table or queue, an unknown `dbutils` method, or
a wrong argument name raises. They model contracts only; Spark and Delta behavior stays in the Apache
tier and Databricks-only behavior in the Databricks tier.

`tests/unit/plain/test_azure_contract.py` runs the same scenarios against a real Azurite emulator when
`FABRICKS_TEST_AZURITE_CONNECTION_STRING` is set (`npx azurite --silent --location "$(mktemp -d)"`,
connection string `UseDevelopmentStorage=true`). It needs Node, not Docker.
````

- [ ] **Step 3: Final verification**

```bash
just test-plain 2>&1 | tail -3
just test-config 2>&1 | tail -3
just test-apache 2>&1 | tail -3
just lint
git status --short   # expect clean
```
Expected: all tiers green, lint clean.

- [ ] **Step 4: Commit**

```bash
git add tests docs
git commit -m "test(apache): use semblance for the runtime fake; document the harness (#220)" \
  -m "Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>"
```

---

## Self-Review Notes (spec coverage)

| Spec requirement | Task |
|---|---|
| A1 lazy `dags.log` table | 2, 3 |
| A2 lazy `databricks.sdk.runtime` + seam guard | 1 |
| Spot 1 conftest finalizers + per-test reset (kinds 3/4) | 4 |
| Spot 2 `_STEP_SESSIONS` | 5 |
| Spot 3 env-reload | 6 |
| Spot 4 Apache `dags.log` fake | 3, 15 |
| Spot 5 `docs/TEST.md` | 7, 15 |
| Strict Spark mock | 0 (spike), 8 |
| `dbutils` fake checked against SDK stub, taskValues, scripted `notebook.run` | 9 |
| Table/Queue fakes (grammar, ordering, delete, exists, factories) | 10 |
| Fixture, handle, registration, `no_real_sleep`, log handler, sleep | 11 |
| Leak tests | 11 |
| Migrate `fake_processor` tests | 12 |
| Local schedule test | 13 |
| Azurite contract test | 14 |
| Follow-up e2e run | not in this plan (spec: separate change) |
| `DagGenerator` in Apache tier | not in this plan (needs real-Spark seams; spec follow-up) |

## Spike results

**Baseline (2026-09-30, before any code change; `uv sync --group test` was needed first):**
- plain: 126 passed (4.0s)
- config: 201 passed (6.8s)
- apache: **not established** — the run was killed (exit 137) before finishing, so the Apache baseline is still unknown. Task 3 edited `test_silver_skip_unchanged.py` without it ever being run; run `just test-apache` (Java 17.0.20 is installed) before merging.

**After Tasks 1-4 + 2b (2026-09-30):** plain 131 passed, config 205 passed.

_(Fill in during later tasks: strict-Spark failing tests; whether `DagProcessor` constructs in the config tier; whether `pytest_plugins` works; Azurite run outcome.)_
