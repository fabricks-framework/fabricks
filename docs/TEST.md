# Testing

Commands below run from `framework/` (`uv sync` once per checkout; the
Apache tier also needs a local Java 17-21; Java 25 fails with
`JAVA_GATEWAY_EXITED`). Run one tier per pytest invocation.
Each tier configures global Spark/context state during collection, so mixing
them makes results order-dependent.

| Tier | Use for | Command |
|---|---|---|
| `tests/unit/plain/` | Pure Python, parsing, and SQL inspection. | `just test-plain` |
| `tests/unit/config/` | Real runtime YAML with a fake Spark session. | `just test-config` |
| `tests/spark/apache/` | Real local Spark and Delta behavior. | `just test-apache` |
| `tests/spark/databricks/` | Databricks-only behavior: notebooks, UC, streaming, masks, and liquid clustering. | `just test-databricks` (deploys the bundle to the `test` workspace). |

Apache tier timing on the remote machine (24 cores), 88 tests: serial about 5m20s; `-n 4` 2m19s to 2m51s over
four runs; `-n 6` 2m09s; `-n 8` 2m04s, all passing. Past 4 workers the gain is small, so
`just test-apache-remote` defaults to 4; `just test-apache` stays serial by default because each worker is a
Spark JVM. Parallel runs are safe because each worker has its own storage under `.worker_cwd/<worker>/`.

The plain and config tiers are local-only: `databricks.yml` excludes `tests/unit/**` from the bundle sync,
and only `tests/spark/databricks/runtests.py` runs on a cluster (see
[decisions/0001](./decisions/0001-plain-and-config-tiers-are-local-only.md)).

`just test-unit` runs plain then config as two separate pytest processes; `just test-all` runs every tier.

To run the Apache tier on a faster machine over ssh, use `just test-apache-remote [TARGET]`. It rsyncs
the working tree, then runs `just test-apache` there. Set `FABRICKS_REMOTE` (ssh host),
`FABRICKS_REMOTE_DIR` and optionally `FABRICKS_REMOTE_JAVA_HOME` in the gitignored `framework/.env`
(loaded by the justfile). The remote machine needs `uv`, `just`, `rsync` and a Java 17-21.

Every `just test-*` recipe also writes its output to
`framework/.logs/<category>/<timestamp>.log` (gitignored; `latest.log` links to
the newest, the newest 20 per category are kept). Set `FABRICKS_TEST_LOG=off`
(for example in `framework/.env`) to disable it.

Use the smallest tier that exercises the changed behavior. SQL generation and
configuration decisions belong in unit tests. Delta correctness belongs in the
Apache tier. A test requiring a live workspace, notebook, Unity Catalog, or
Databricks execution engine belongs in the Databricks tier.

The Databricks suite uses `tests/spark/databricks/runtime/` as its fixture
runtime. `runtests.py` seeds data, resets the runtime, and runs the suite.
Deploy the bundle before running it; local runs cannot validate this tier.

For a new behavior, add the smallest regression test that would fail if the
behavior regressed. Do not add a Databricks test when a unit or Apache test can
prove the same behavior.

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
- Per-test state is fresh or reset: the shared bootstrap mocks (`SPARK`, `dbutils`) are reset around
  every config-tier test.
- Tier-process isolation stays: run each tier in its own pytest invocation.

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
