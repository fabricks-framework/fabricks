# Integration tests — Databricks runbook

This document describes how to build the `fabricks` wheel from the `framework` package, upload it to Databricks (DBFS), install it on a cluster, and run notebooks or integration tests on Databricks.

Prerequisites
- Python 3.9–3.10 (matches `pyproject.toml`).
- `hatch` installed (for building): `pip install hatch`.
- Databricks CLI configured or `databricks-sdk` available and authenticated.
- `databricks-cli` (optional) for simple DBFS and libraries operations: `pip install databricks-cli`.

1) Build the wheel
- From the repository root (where `framework/pyproject.toml` lives) run:

```
cd framework
hatch build -t wheel
```

- The wheel will be written to `dist/` (e.g. `dist/fabricks-3.0.12-py3-none-any.whl`).

2) Upload wheel to DBFS
- Using the Databricks CLI (`databricks`):

```
# Put the wheel under DBFS, e.g. /FileStore/wheels/
databricks fs cp dist/fabricks-3.0.12-py3-none-any.whl dbfs:/FileStore/wheels/fabricks-3.0.12-py3-none-any.whl
```

- Or using the Databricks Python SDK:

```
from databricks import sdk
client = sdk.create_client()
with open('dist/fabricks-3.0.12-py3-none-any.whl', 'rb') as f:
    client.dbfs.put('/FileStore/wheels/fabricks-3.0.12-py3-none-any.whl', f.read())
```

3) Install the wheel on a cluster
- Via Databricks CLI (clusters API):

```
# Replace <cluster-id>
databricks libraries install --cluster-id <cluster-id> --whl dbfs:/FileStore/wheels/fabricks-3.0.12-py3-none-any.whl
```

- Via UI: Go to your cluster -> Libraries -> Install -> Upload or DBFS -> Provide the `dbfs:/FileStore/wheels/....whl` path.

4) Run a notebook or job that uses the library
- Create or import the notebook you want to run (for example, `framework/tests/integration/runtime/fabricks/notebooks/initialize.py` converted to a Databricks notebook or a small wrapper notebook).

- Submit a job via CLI (using the Jobs 2.0/3.0 API) — example JSON payload saved as `run-notebook.json`:

```
{
  "name": "fabricks-integration-run",
  "existing_cluster_id": "<cluster-id>",
  "notebook_task": {
    "notebook_path": "/Users/you@example.com/path/to/notebook"
  }
}

databricks jobs create --json-file run-notebook.json
databricks jobs run-now --job-id <job-id>
```

- Or use the Python SDK to submit a run (simplified):

```
from databricks import sdk
client = sdk.create_client()
resp = client.jobs.create({
  'name': 'fabricks-integration-run',
  'existing_cluster_id': '<cluster-id>',
  'notebook_task': {'notebook_path': '/Users/you@example.com/path/to/notebook'}
})
client.jobs.run_now(resp.job_id)
```

5) Run pytest integration tests (two approaches)
- Local smoke run (requires databricks-connect configured and matching runtime):

```
# from project root
pytest framework/tests/integration -k databricks -q
```

- Full Databricks-based run
  - Build and upload the wheel (steps 1–2).
  - Install wheel on a test cluster (step 3).
  - Use the Jobs API to run the notebooks that exercise the integration tests; capture results from job run output.

Notes and tips
- Pin the wheel filename or use a stable path like `dbfs:/FileStore/wheels/latest/fabricks.whl` and overwrite when you upload.
- If tests require credentials or a `conf.uc.fabricks.yml`, provide them via environment variables or the cluster init script.
- For iterative development prefer the Databricks UI to install the wheel and manually run notebooks for faster feedback.
- To debug, check the driver logs and job run output in the Jobs UI.

Helpful commands
- List DBFS files: `databricks fs ls dbfs:/FileStore/wheels/`
- Show job runs: `databricks runs list --job-id <job-id>`

If you want, I can also add example job JSONs, a small wrapper notebook that runs the integration tests on Databricks, or CI pipeline snippets to automate the upload and run steps.
