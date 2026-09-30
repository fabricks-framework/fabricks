# Semblance Test Harness — Design

**Issue:** [fabricks-framework/fabricks#220](https://github.com/fabricks-framework/fabricks/issues/220)
**Related:** [docs/superpowers/research/2026-09-28-databricks-labs-pytester.md](../research/2026-09-28-databricks-labs-pytester.md) (rejected as a dependency — real-workspace provisioning, not a local mock)

## Problem

Several tests mutate process-wide mocking state without restoring it:
process-wide `sys.modules` substitutions, a mutable shared fake Spark
session, an unrestored resolver cache entry, and an environment-reload
fixture whose finalizer runs its last module reload before the environment
value it depends on is actually restored. The one-tier-per-pytest-process
policy limits the blast radius, but state still leaks between tests within
a tier, producing order-dependent outcomes and, in one case, permanently
replacing a real module with a fake for the rest of the process.

## Scope

Keep the one-tier-per-pytest-process rule and the existing external-boundary
fakes. Add a shared, function-scoped harness that models only the contracts Fabricks
consumes. It is named `semblance` (package `tests/semblance/`, fixture
`semblance`) to avoid confusion with the Fabricks runtime:

- `databricks.sdk.runtime` bindings for `spark`/`dbutils`.
- `dbutils` fakes for widgets, secrets, filesystem, notebook exit — the four
  contracts the issue names — plus two the schedule paths need in practice:
  `jobs.taskValues` and a scripted `notebook.run` (see Components).
- In-memory Azure Table behavior: partition/row keys, the filter grammar
  Fabricks generates (below), upsert, delete.
- In-memory Azure Queue behavior: enqueue, dequeue, ordering, and only the
  visibility/retry behavior current schedule paths need.
- The existing `LocalFileSharePath` is used as-is (real code, selected by
  `FABRICKS_ENVIRONMENT=docker`); the harness adds nothing for it.
- A small handle API (below) so a test can seed and assert without writing raw
  entity dicts.

No general Azure/Databricks platform emulator, no Unity Catalog, streaming,
remote Workspace API, or engine-specific SQL support — those stay Apache- or
live-Databricks-tier concerns.

Fix the five affected spots the issue names, update `docs/TEST.md`, and make
three small production-code seam changes (Step 0) that remove most of the
patching the harness would otherwise need.

**Objective.** The point of this work is that a contributor can write a local
test against the Databricks/Azure environment in a few lines: request one
fixture, seed state through a handle, run real Fabricks code, assert on the
handle. Every design choice below is judged against that.

## Step 0: Production Seams

Much of the patching in the tiers exists only because two pieces of production
code do work or bind names at import time. Fix them first; they are mechanical.

- **A1. `dags/log.py` must not do I/O at import.** Today `table = get_table()`
  runs `FABRICKS_STORAGE.get_storage_account()` and reads secrets when the
  module is first imported, which is why the config tier and
  `test_silver_skip_unchanged.py` replace `fabricks.core.dags.log` wholesale in
  `sys.modules`. Change `AzureTableLogHandler` (`utils/log.py:97`) to accept
  either an `AzureTable` or a zero-argument factory, and expose `table` as a
  property that resolves the factory once on first access into a private
  `_table` (`None` until resolved). `dags/log.py` passes `get_table` (the
  function, not its result).
  `LOGGER` and `TABLE_LOG_HANDLER` stay module-level constants, so the ~35 use
  sites and the by-value imports (`base`, `generator`, `terminator`, `run`,
  `processor`) do not change. Consequences:
  - importing the real `dags.log` is safe in every tier; the wholesale fakes in
    `unit/config/conftest.py` and `test_silver_skip_unchanged.py` are deleted;
  - `LOGGER` is a real logger, so log *records* become capturable and
    `TABLE_LOG_HANDLER._table` can be pointed at a fake table with a plain
    `monkeypatch.setattr` in any tier (the private attribute, not the
    property: `monkeypatch.setattr` reads the old value first, and reading the
    property would resolve the factory and fail locally);
  - caveat: `get_logger` clears the root logger's handlers when it runs, so
    `fabricks.core.dags.log` must first be imported at collection time (as it is
    today via `dags.run`), never lazily inside a test, or it would strip pytest's
    `caplog` handler.
- **A2. No import-time `from databricks.sdk.runtime import …`.** Move the three
  (`core/dags/processor.py:8`, `core/dags/run.py:5`, `core/schedules/dags.py:1`)
  inside the functions that use them, matching the six modules that already do
  this (`secret.py`, `invoker.py`, `helpers.py`, …). The fixture can then swap
  `sys.modules["databricks.sdk.runtime"]` and nothing holds a stale copy, so the
  `_BINDINGS` table is not needed. A small seam-guard test (plain tier, `ast`)
  fails if any module under `fabricks/`, except `fabricks/api/notebooks/`,
  gains a module-level import of `databricks.sdk.runtime`.

Not done here: the 21 by-value `from fabricks.context import SPARK` imports.
Replacing them with an accessor is a much larger change; the bootstrap-mock
reset in "Binding points" covers them.

## Approach

Fakes only at the true external boundary — the Azure SDK client classes and
`dbutils` — patched in with pytest's `monkeypatch`, built fresh per test.
Everything above that boundary (`AzureTable`, `AzureQueue`, `LocalFileSharePath`,
`DagProcessor`) runs its real code against the fakes.

### Binding points: what can and cannot be replaced per test

"Fresh per test" only holds for a name the fixture can rebind. Names copied
by value at import time hold whatever object existed then, so the fixture must
know each one. There are four kinds (kind 1 is removed by Step 0):

1. **Import-time `from databricks.sdk.runtime import …`** — three today
   (`core/dags/processor.py:8`, `core/dags/run.py:5`,
   `core/schedules/dags.py:1`). Step 0 (A2) moves them inside functions, so
   this kind no longer exists and the fixture needs no per-module binding
   table. The seam-guard test keeps it that way. `fabricks/api/notebooks/` is
   excluded from the guard: those six files are Databricks notebook entry
   points that import `dbutils` at module level by design and are never
   imported by unit tiers.
2. **In-function imports** — `from databricks.sdk.runtime import dbutils`
   (`secret.py`, `invoker.py`, `file_share.py`, `helpers.py`, `log.py`,
   `bronze.py`) and `from fabricks.utils.spark import dbutils`
   (`file_share.py:22`) — resolved from `sys.modules` at call time. The
   fixture always installs its own fresh `databricks.sdk.runtime` module with
   `monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", <new module>)`
   carrying the fake `dbutils`/`spark`, and `monkeypatch.setattr`s the
   `dbutils` attribute on the (mocked or real) `fabricks.utils.spark` module.
   It must not rely on a tier conftest having pre-faked
   `databricks.sdk.runtime`: only the config conftest does; the plain conftest
   does not, and importing the real module there tries to authenticate
   against a workspace.
3. **`fabricks.context.SPARK`** — copied by value into 21 modules via
   `from fabricks.context import SPARK`, and built once at first import. It
   cannot be replaced per test. This one bootstrap `MagicMock` is *reset*
   instead of replaced (`reset_mock(return_value=True, side_effect=True)` in
   an autouse fixture). This is a deliberate exception to "fresh by
   construction"; a leak test covers it.
4. **`fabricks.context.DBUTILS`** (`spark_session.py:87`, imported by value in
   `core/dags/utils.py:4`) **and the config conftest's shared
   `_fake_dbutils`** (held by the mocked `fabricks.utils.spark` module and
   therefore also what `DBUTILS` resolves to). Same limitation and same
   treatment as `SPARK`: reset in the same autouse fixture, covered by the
   same leak test. `semblance`' `FakeDbutils` is separate and fresh per
   test; code paths that read `DBUTILS` still see the bootstrap mock, so a
   test needing `DBUTILS` behavior patches that one attribute explicitly.

Rejected alternatives:

- **Standalone context-manager harness** (usable outside pytest): reinvents
  `monkeypatch`'s LIFO undo stack by hand, which is more code and reintroduces
  the exact "forgot to restore" bug class this issue exists to remove.
- **Session-scoped fakes with a manually maintained `.reset()`** as the general
  strategy: every fake's internal state has to be reset by hand every test; a
  missed field silently reintroduces leakage. Used only for the one object in
  kind 3 above where replacement is impossible.

## Components

New package `framework/tests/semblance/`, sibling to the existing
`framework/tests/tier_policy.py`:

- **`azure_fakes.py`, `dbutils_fake.py`, `stub.py`** (fakes; `stub.py` loads the
  SDK's `dbutils_stub` by file path and holds the signature check)
  - `FakeDbutils` — `.widgets` (`text`/`get` backed by a dict; `.text(name,
    defaultValue, label=None)` only sets `name` if it has no value yet,
    matching real Databricks semantics), `.secrets` (`get`/`listScopes` backed by a dict),
    `.fs` implementing only what `FileSharePath` calls (`file_share.py:25-140`):
    `ls(path)` returning `FileInfo`-like objects (`.path`, `.name`, `.size`,
    `.modificationTime`, `.isDir()`) and `rm(path, recurse=True)`, both over
    `pathlib` rooted at a `tmp_path`, and `.notebook.exit(value)` raising an internal
    `_NotebookExit(value)`. Any other attribute raises `AttributeError`
    (no `MagicMock` defaults), so an unsupported call fails loudly. Two more,
    because the schedule paths call them:
    - **`.jobs.taskValues`** (`schedules/dags.py`, `dags/run.py`): dict-backed
      `set(key, value)`; `get(taskKey, key, default=None, debugValue=None)`
      raises `TypeError` when the key is unset and no default is given, which
      is what real `dbutils` does outside a job and what those call sites
      catch (`except (TypeError, IllegalArgumentException, ValueError)`) before
      falling back to widgets. Without it, `AttributeError` would escape that
      handler and every local schedule test would fail.
    - **`.notebook.run(path, timeout_seconds, arguments)`** (`processor.py:157`):
      scripted, not emulated. Records each call and returns a result
      registered with `on_run(...)` (a status string such as `"success"`); an
      unregistered call raises `AssertionError` naming the path and arguments.
      Executing real notebooks in-process stays out of scope.
  - `FakeTableServiceClient` / `FakeTableClient` — in-memory dict keyed by
    `(PartitionKey, RowKey)`. Supports `create_table_if_not_exists`,
    `query_entities`, `submit_transaction` (upsert/delete batches),
    `delete_table`. `query_entities` returns rows sorted by `(PartitionKey,
    RowKey)`, as the real service does, not insertion order, so a test cannot
    accidentally depend on insertion order.
    - **SDK shape:** `AzureTable` uses both construction paths, so the
      patched `TableServiceClient` stand-in must support
      `from_connection_string(...)` (classmethod), the
      `(endpoint=…, credential=…)` constructor, and `close()`. `create_table_if_not_exists`
      returns the `FakeTableClient` for that table name; state lives in the
      service so two calls for the same name share rows.
    - **Delete semantics:** deleting a row that does not exist raises
      `ResourceNotFoundError`, as the real service does — not a silent no-op.
    - **Filter grammar:** the empty string (all rows), or one or more
      `Field eq 'value'` clauses joined by ` and `. Fabricks generates
      exactly these (`processor.py:72,100,129,189`, `base.py:55-57`,
      `azure_table.py:143`), e.g.
      `PartitionKey eq 'dependencies' and JobId eq '…' and Status eq 'pending'`.
      Any other operator or shape raises `NotImplementedError`.
    - **Fail-loud vs. retry:** every `AzureTable` method is wrapped in
      `tenacity` retrying any `Exception` (3 attempts, exponential wait, 1s
      minimum). A raised `NotImplementedError` would therefore only surface
      after ~3s of real sleep. Tenacity's default sleep is `time.sleep`, so
      the fixture `monkeypatch`es `time.sleep` to a no-op, which also covers
      the unconditional `time.sleep(60)` in `generator.py`. This absorbs the
      config conftest's existing `no_real_sleep` fixture: extract it into
      `tests/semblance/` and reuse it from `semblance` rather than
      keeping two mechanisms. Unsupported operations still surface the error
      after the (now instant) retries.
  - `FakeQueueClient` (one queue) and `FakeQueueClients` (the registry patched
    in place of the `QueueClient` class, keyed by `queue_name`) — FIFO.
    - **SDK shape:** `QueueClient.from_connection_string(conn, queue_name=…)`
      (classmethod) and the `(account_url=…, queue_name=…, credential=…)`
      constructor both resolve to the same per-name `FakeQueueClient`, which
      also supports `close()`.
    - Supports `create_queue` (a no-op on an existing queue: the Azure Queue
      REST API returns 204 when the metadata is identical and 409 only when it
      differs; metadata is not modelled. The contract test settles this on
      Azurite), `send_message` (raises `ResourceNotFoundError` if the queue
      was never created, like the real service), `receive_message` (returns an object with `.content`, or
      `None` when empty), `delete_message(msg)`, `clear_messages`,
      `delete_queue`. No visibility timeout and no dequeue count: `AzureQueue`
      only ever receives-then-deletes, so a received message is removed
      immediately.
- **`fixture.py`** — function-scoped `semblance` pytest fixture. Builds
  fresh `FakeDbutils`/`FakeTableServiceClient`/`FakeQueueClient` instances and
  `monkeypatch`es:
  `fabricks.utils.azure_table.TableServiceClient`,
  `fabricks.utils.azure_queue.QueueClient`, a fresh
  `sys.modules["databricks.sdk.runtime"]` carrying the fake `dbutils`/`spark`,
  `time.sleep`, and (after Step 0 A1) `TABLE_LOG_HANDLER.table`, which it sets
  to a real `AzureTable("dags", …)` on the fake service client, in any tier.
  It touches only Azure and `dbutils`, never Spark, so it works unchanged in
  the Apache tier next to real Spark. Returns the handle below. Teardown is
  `monkeypatch`'s own undo stack — nothing manual. Ordering: the fixture
  depends on `monkeypatch` (so its undo runs last); the autouse
  `SPARK`/`DBUTILS` reset from spot 1 runs independently of it.
- **The handle** (what `semblance` returns; this is the surface a test
  author sees, so it stays small and is documented with an example in
  `docs/TEST.md`):

  ```python
  def test_dependency_dispatch(semblance):
      semblance.widgets["schedule_id"] = "s1"                    # dbutils.widgets.get
      semblance.secrets[("scope", "key")] = "value"              # dbutils.secrets.get
      semblance.task_values["schedule"] = "daily"                # dbutils.jobs.taskValues
      semblance.on_notebook_run(returns="success")               # scripts dbutils.notebook.run
      semblance.queue("qsilvers1").create()
      semblance.table("ts1").seed([status_row("job-1")])         # tests.semblance.schedule

      ...  # drive real DagProcessor / schedules.dags code

      semblance.table("ts1").rows(PartitionKey="statuses", Status="waiting")   # -> list[dict]
      semblance.queue("qsilvers1").sent        # every message ever sent, in order
      semblance.queue("qsilvers1").pending     # not yet received
      semblance.notebook_calls                 # [NotebookCall(path, timeout_seconds, arguments)]
      semblance.fs_root                        # tmp_path backing dbutils.fs (sandboxed)
  ```

  Seeding goes through plain dicts and `on_notebook_run` on the handle, not
  through `semblance.dbutils.*.set(...)`: real `dbutils` has no `widgets.set`,
  and the conformance test forbids the fake from offering methods the SDK stub
  lacks. `semblance.dbutils` is only what production code sees.

  `.sent` and `.pending` are separate because `AzureQueue.receive` deletes what
  it reads, so "what was dispatched" would otherwise be lost. `rows(**where)`
  uses the same equality-only filter as the fake's query, so a test cannot
  assert with a shape production could not send. Row-shape helpers for the
  dependency and status partitions (`dependency_row`, `status_row`) live in
  `tests/semblance/schedule.py` and are written against the row shapes
  `DagGenerator` produces; they replace the ad-hoc dicts in
  `unit/config/_dag_processor_helpers.py`.
- **Registration and imports** — the fixture is exposed to every tier through
  `pytest_plugins = ["tests.semblance.fixture"]` in the root
  `tests/conftest.py` (the only place `pytest_plugins` is allowed). It is
  loaded at collection, before any tier conftest bootstraps the fake
  `fabricks` environment, so nothing under `tests/semblance/` may import
  `fabricks` at module level: targets are given to `monkeypatch.setattr` as
  dotted strings and any import happens inside the fixture body. Apache and
  Databricks tiers load the plugin but never request the fixture.

## Strictness: Spark and the Databricks Runtime

A fake that accepts anything lets a test pass while the code calls something
that does not exist. Both Databricks-facing fakes are checked against the real
API shape. Neither checks behaviour: that stays the Apache and Databricks
tiers' job.

**Spark (config tier).** `unit/config/conftest.py` builds the shared session as
a bare `MagicMock`. Replace it with `MagicMock(spec=SparkSession)`, and the
builder with `MagicMock(spec=SparkSession.Builder)`, so an attribute that does
not exist on the real class raises `AttributeError` (`SPARK.sqll(...)`). The
same object backs `fabricks.context.SPARK`, `fabricks.utils.spark.spark` and
the `spark` on the faked `databricks.sdk.runtime`, so one change covers all
three. `reset_mock` keeps the spec, so the per-test reset from spot 1 is
unchanged.

Limits, stated plainly:
- `spec=` guards one level. `spark.conf`, `spark.read` and `spark.sql(...)`
  are still plain `MagicMock`s, so `spark.conf.sett(...)` is not caught.
  `create_autospec(SparkSession, instance=True)` would go deeper but turns the
  class's properties (`conf`, `read`, `catalog`) into non-callable specs and
  breaks existing uses such as `session.conf.set.assert_any_call`, so it is
  not used. Giving `conf` a `spec=RuntimeConfig` and `read` a
  `spec=DataFrameReader` is a cheap follow-up if the top-level guard proves
  useful.
- Results are still not `DataFrame`-shaped. Tests that need real query results
  belong in the Apache tier.
- Fallout is unknown until the suite is run against it. Existing config tests
  may touch attributes that do not exist on `SparkSession` (or set them
  directly). Delivery step 2 starts by running the config tier with the
  spec'd mock. If the fixes are trivial, they go in; if not, the strict mock
  is split into its own change so it does not block the harness.

**`dbutils` and the runtime (semblance).** The installed `databricks-sdk`
ships the real API as type stubs (`databricks.sdk.runtime.dbutils_stub`, e.g.
`notebook.run(path, timeout_seconds, arguments)`,
`jobs.taskValues.get(taskKey, key, default, debugValue)`,
`widgets.text(name, defaultValue, label)`, `fs.rm(dir, recurse)`). `FakeDbutils`
uses them two ways:
- **At call time:** each fake method is wrapped so its arguments are bound
  against the stub method's signature (`inspect.signature(stub).bind(...)`)
  before the fake logic runs. A wrong or misspelled argument fails in the test,
  not in production. This is the "parameter-aware fake" the issue asks for.
- **As a conformance test** (plain tier): every namespace and method exposed by
  `FakeDbutils` must exist on the stub with a compatible signature, so the fake
  cannot offer something real `dbutils` does not have, and an SDK upgrade that
  changes a signature fails one test. The stub is typing-only, so it can lag
  the real runtime; the pinned SDK (0.108.0 in `uv.lock` today) is the
  reference and the test names it.

The `spark` handed out by the fake `databricks.sdk.runtime` module is the same
spec'd session mock as above.

## Data Flow

A test requests `semblance` → the fixture patches the SDK-client boundary
and `databricks.sdk.runtime` → the test drives real `fabricks.core.schedules.dags` /
`DagProcessor` / `AzureTable` / `AzureQueue` code → assertions read the fake's
in-memory state directly (dispatched queue messages, table rows) → teardown
reverts everything via `monkeypatch`.

## The Five Affected Spots

Only the Table/Queue/dbutils pieces above are genuinely new shared harness
code. The rest are standalone restoration fixes:

1. **`tests/unit/config/conftest.py`, `tests/unit/plain/conftest.py`** — the
   process-wide `sys.modules` substitutions and `SparkSession.builder =
   _fake_builder` happen at import time, before any fixture runs (collection
   imports test modules that transitively import `fabricks.context`), so they
   cannot become fixture-scoped and stay per the issue's own scope note. What
   changes:
   - A session-scoped autouse fixture with a `yield` restores, at process end,
     the original `SparkSession.builder` and every `sys.modules` entry the
     conftest replaced — the finalizer both conftests lack today. Honest
     value: the `runtests.py` runners call `pytest.main` once per process, so
     this only protects a process that runs pytest more than once (a REPL, an
     IDE runner, a nested `pytester` run). It is cheap and the issue asks for
     it; keep it, with a comment saying why, so it isn't later deleted as dead
     code.
   - A function-scoped autouse fixture resets the bootstrap `SPARK` mock and
     the shared `_fake_dbutils`/`DBUTILS` mock (binding kinds 3 and 4 above)
     and rebuilds `SparkSession.builder` per test via `monkeypatch.setattr`,
     so a `return_value`/`side_effect` configured in one test cannot reach the
     next. `FakeDbutils` is replaced fresh per test by `semblance`, not
     reset.
   - After Step 0 A1 the config conftest's `sys.modules["fabricks.core.dags.log"]`
     fake is deleted (the real module is safe to import), so it is not among
     the entries the session finalizer has to restore.
2. **`tests/unit/config/test_build_step_spark_session_connect.py`** —
   `monkeypatch.setattr(resolver, "_STEP_SESSIONS", {})` replaces the pop. It
   restores the original dict on teardown in every outcome and needs no custom
   fixture. Safe because `_STEP_SESSIONS` is a plain module-level dict
   (`resolver.py:53`) read only via the module global, so nothing else holds a
   reference to the original object.
3. **`tests/unit/plain/test_local_file_share_path.py`** — bugfix only. The
   `set_environment` finalizer deletes the env var, reloads both modules, and
   only afterward does `monkeypatch`'s own teardown restore the original value.
   Fix: call `monkeypatch.undo()` at the start of the finalizer, before the two
   reloads, so the modules reload against the environment the process will
   actually keep. (`monkeypatch.undo()` is public API and idempotent, so the
   later automatic teardown is harmless.)
4. **`tests/spark/apache/test_silver_skip_unchanged.py`** — the module-level
   `if … not in sys.modules` block sets two fakes permanently:
   `fabricks.core.dags.log` and `databricks.sdk.runtime`. With Step 0 both
   reasons for them are gone: importing `dags.log` no longer does I/O (A1) and
   `dags.run`/`processor` no longer import `databricks.sdk.runtime` at module
   level (A2). Fix: delete the whole block, and import `dags.run` normally.
   This also removes the by-value `LOGGER` problem, since `LOGGER` is now the
   real logger. Where the test needs a `dbutils`, it requests `semblance`.
   If Step 0 is not done first, the fallback is the scoped-fixture version:
   `monkeypatch.setitem` both fakes inside a fixture, import `dags.run` there,
   and `monkeypatch.delitem` the fake-bound `fabricks.core.dags.*` modules on
   teardown so a later import gets the real ones.
5. **`docs/TEST.md`** — add the mocking-strategy paragraph: mock external
   boundaries, keep business behavior real, make fakes honor parameters under
   test, use real Spark/Delta for Spark semantics, restore process-wide
   state, retain tier-process isolation. Also state the usage rule: new tests
   use `semblance` for Azure/`dbutils` and never write to `sys.modules`
   or `os.environ` directly (use `monkeypatch`). The current text says each
   tier "configures global Spark/context state during collection"; keep it and
   add that per-test state is restored by the fixtures.

## New Coverage Enabled

A local schedule test (`tests/unit/config/test_local_schedule.py`) (alongside the existing
`test_dag_*`/`test_dags_run_status.py` files) that asserts, without a live
Azure service and using `semblance`:

- dispatched queue messages (`AzureQueue.send`);
- dependency-table and status-table rows (`DagProcessor.get_azure_table` →
  `AzureTable` → `FakeTableClient`), driven through the dict-row paths in
  `processor.py`/`run.py`;
- log-table rows Fabricks upserts as dicts through `TABLE_LOG_HANDLER.table`
  (read back by `base.py:59` `get_logs`).

Log rows need no special step after Step 0 A1: the real `dags.log` imports
safely, `LOGGER` is a real logger, and `semblance` points
`TABLE_LOG_HANDLER.table` at a real `AzureTable("dags", …)` on the fake
service client. Individual log *records* emitted through `LOGGER` with the
`target`/`job`/`step` extras therefore reach the handler and, after
`TABLE_LOG_HANDLER.flush()`, the fake table, so a test can assert on them with
`fakes.table("dags").rows(...)`. (`FABRICKS_STORAGE.get_storage_account()` is
unimplemented on `LocalFileSharePath`, which is why the handler's table must be
replaced by the fixture rather than resolved from its factory in local tests.)

**Not covered in the config tier: `DagGenerator` (`generator.py`).** It builds
its dependency, status and log rows with `SPARK.sql(...)` and passes the
resulting DataFrames to `AzureTable.upsert`, which calls `toPandas()`. Under
the fake config-tier Spark session those are `MagicMock`s, so nothing
meaningful reaches the fake table (the fake rejects rows without a string
`PartitionKey`, so this fails loudly rather than passing vacuously). Testing
generation needs real Spark and belongs in the Apache tier, where
`semblance` supplies the Azure side next to real Spark and Delta.

## Testing

- Harness tests live inside a tier (an untiered directory gets no conftest bootstrap): pure-fake tests in
  `tests/unit/plain/test_semblance.py`, fixture/leak/guard tests in
  `tests/unit/config/test_semblance_fixture.py`. Together they prove the harness itself:
  - one leak test written as two sequential tests (the first writes, the
    second asserts a clean slate), each checking *all* state at once: table
    rows, queue messages, widget values, the bootstrap `SPARK` mock
    (`return_value`, `side_effect`, and an assigned attribute) and the
    `DBUTILS`/`_fake_dbutils` mock (the issue's own acceptance criterion);
  - a partition/row-key upsert+query round trip, including the multi-clause
    `and` filter and the empty filter;
  - queue send/receive FIFO ordering and empty-queue behavior;
  - an unsupported filter operator raises `NotImplementedError` (through
    `AzureTable.query`, completing well under the 3s retry backoff), deleting
    a missing row raises `ResourceNotFoundError`, sending to a queue that was
    never created raises `ResourceNotFoundError`, creating an existing queue is
    a no-op, and an unknown `FakeDbutils` attribute raises `AttributeError`;
  - `semblance` works in the plain tier (no pre-faked
    `databricks.sdk.runtime` in `sys.modules`) as well as the config tier;
  - the seam-guard test from Step 0 A2 (plain tier, `ast`);
  - `dbutils` conformance against `databricks.sdk.runtime.dbutils_stub`, and a
    call with a wrong argument name raises `TypeError` at the fake;
  - `SPARK.<nonexistent>` raises `AttributeError` in the config tier, while a
    real `SparkSession` method still works;
  - handle-API tests: `.sent` keeps messages after `receive`, `.pending` does
    not; `rows(**where)` rejects a non-equality filter; unregistered
    `notebook.run` raises; `taskValues.get` on an unset key raises `TypeError`.
- **Contract test against Azurite** (`tests/unit/plain/test_azure_contract.py`).
  The in-memory fakes will drift from real Azure unless something compares
  them. One parametrized module runs the same scenarios through the real
  `AzureTable`/`AzureQueue` wrappers with `backend` in `["fake", "azurite"]`;
  the `azurite` case is skipped unless
  `FABRICKS_TEST_AZURITE_CONNECTION_STRING` is set (Azurite's dev string is
  `UseDevelopmentStorage=true`; clients construct with it against the pinned
  `azure-data-tables` 12.7.0 and `azure-storage-queue` 12.15.0, table on
  `127.0.0.1:10002`, queue on `127.0.0.1:10001`). Both paths go through
  `connection_string=`, so the fake backend exercises the same
  `from_connection_string` code path. Scenarios: upsert/query round trip,
  multi-clause filter and empty filter, result order `(PartitionKey, RowKey)`,
  delete of a missing row → an Azure `HttpResponseError` (a transaction
  surfaces it as `TableTransactionError`; the fake raises its subclass
  `ResourceNotFoundError`), a second `create_queue` keeping the queue and its
  messages, and queue send/receive/delete. Assert only what Azure
  guarantees for Azurite (each message received exactly once; strict FIFO is
  asserted for the fake only, since real Azure queues do not guarantee it).
  Each Azurite run uses uuid-suffixed table and queue names and deletes them on
  teardown. Default CI and `just test-plain` need neither Docker nor Azurite.
  Azurite is opt-in and runs without containers: `npx azurite --silent
  --location <tmpdir>` (needs Node only), documented in `docs/TEST.md` and
  usable manually or in an optional CI job.
- **Migrate the hand-rolled fakes as the proof of ease.**
  `unit/config/_dag_processor_helpers.py` builds a `DagProcessor` with
  `__new__` and `MagicMock` queue/table objects, which the issue calls out as
  scattered permissive fakes. Rewrite `test_dag_receive_status.py` and
  `test_dag_receive_skips_unchanged.py` on `semblance` and delete the
  helper. Open question to verify during implementation: whether a real
  `DagProcessor.__init__` can run in the config tier (it resolves the storage
  account and connection info). If not, that resolution is the next seam to
  fix (as in Step 0), not a reason to keep `__new__`.
- The local-schedule test per "New Coverage Enabled" above.
- Each of the five affected-spot fixes gets or keeps a regression test proving
  the specific restoration now holds, e.g. run the Spark Connect test twice in
  one process and assert `_STEP_SESSIONS` is unchanged after; run the
  env-reload test twice and assert the second run's reload sees the correct
  pre-test environment; assert `"fabricks.core.dags.log"` is not a `MagicMock`
  in `sys.modules` after the Apache skip-unchanged test; and a plain-tier test
  that importing `fabricks.core.dags.log` with no Azure environment succeeds
  and performs no I/O (Step 0 A1).

## Out of Scope

- Any general Azure/Databricks platform emulator.
- Unity Catalog, streaming, remote Workspace API, or engine-specific SQL
  fakes — Apache/live-Databricks-tier concerns.
- Adding `databricks-labs-pytester` or `algattik/databricks_test` as
  dependencies (see linked research doc).
- **Executing real notebooks in-process.** An earlier draft had
  `dbutils.notebook.run` running real `.py` notebooks via `runpy` (which would
  make `JobInvoker.pre_run`/`post_run` testable locally). `notebook.run` is
  only *scripted* here (records the call, returns a registered status); real
  execution is the largest piece of new code and warrants its own issue (file
  it and link the number here when this spec is implemented). Existing tests
  that stub `dbutils.notebook.run` via `monkeypatch.setattr`
  (`test_invoker_transient_retry.py`, `test_job_run_transient_retry.py`) keep
  working unchanged.
- Multi-notebook Jobs-style scheduling/orchestration semantics — real
  Databricks Jobs stay a live-Databricks-tier concern.
- Strictness below the top level of the Spark mock (`spark.conf`,
  `spark.read`, `DataFrame` results) and call-signature checking on `spark`
  methods; see "Strictness" for why the guard stops one level deep.
- Replacing the 21 by-value `from fabricks.context import SPARK` imports with an
  accessor (see Step 0).

## Follow-up: Local End-to-End Schedule Run (not in this change)

Once the harness exists, one Apache-tier test could run a whole schedule
locally, with merge and orchestration together and no workspace:

`DagGenerator` (real Spark) writes dependency, status and log rows to the fake
Azure table and clears the fake queues → `DagProcessor` dispatches through the
fake queue → a real Silver job runs on local Delta and merges → assertions read
the Delta table and `semblance.table("dags")` / `.queue(...).sent`.

Why it may be cheap: `DagProcessor` only calls `dbutils.notebook.run` when
`self.notebook` is true. Otherwise it calls `run(job=…, schedule_id=…,
schedule=…)` in-process (`processor.py`), so no scripted notebook is needed for
this path. `time.sleep(60)` in `generator.py` is already neutralised by the
fixture.

Open questions to answer before scoping it, none needed for this change:
- Can `DagGenerator`/`DagProcessor.__init__` run against local storage, given
  they resolve the storage account and connection info
  (`FABRICKS_STORAGE.get_storage_account()` is unimplemented on
  `LocalFileSharePath`)? If not, that resolution is another Step 0 style seam.
- What is the smallest runtime config (steps, jobs, one Silver merge) that
  gives a meaningful run without pulling in the whole fixture runtime?
- Local Delta run time: keep it to one or two jobs so it stays in the Apache
  tier's budget.

It is not a substitute for the Databricks tier: OSS Delta, no Unity Catalog, no
real `notebook.run` timing or failure modes.

## Delivery

Two changes, in this order, so the small ones land without waiting on the
larger one:

1. **Seams and restoration fixes** — Step 0 (A1, A2) with the seam-guard and
   import-safety tests, plus spots 1–4 (spot 4 becomes "delete the fakes"), and
   the spot 5 doc text that does not depend on the harness. Closes the actual
   state leaks; no new fake code.
2. **The harness** — first, run the config tier against the spec'd Spark mock
   and fix or split off the fallout (see "Strictness"); then `tests/semblance/`
   (fakes with stub-checked signatures, fixture, handle, schedule seed
   helpers), the Azurite contract test, the migrated `fake_processor`
   tests, the local-schedule test, and the `docs/TEST.md` worked example and
   Azurite recipe.
