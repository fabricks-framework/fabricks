# Testing

Commands below run from `framework/` (`uv sync` once per checkout; the
Apache tier also needs a local Java 17+). Run one tier per pytest invocation.
Each tier configures global Spark/context state during collection, so mixing
them makes results order-dependent.

| Tier | Use for | Command |
|---|---|---|
| `tests/unit/plain/` | Pure Python, parsing, and SQL inspection. | `just test-plain` |
| `tests/unit/config/` | Real runtime YAML with a fake Spark session. | `just test-config` |
| `tests/spark/apache/` | Real local Spark and Delta behavior. | `just test-apache` |
| `tests/spark/databricks/` | Databricks-only behavior: notebooks, UC, streaming, masks, and liquid clustering. | `just test-databricks` (deploys the bundle to the `test` workspace). |

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
