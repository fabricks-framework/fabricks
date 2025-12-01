# Integration tests — Databricks runbook

This document describes how to build the `fabricks` wheel from the `framework` package, upload it to Databricks (DBFS), install it on a cluster, and run notebooks or integration tests on Databricks.

**Prerequisites**
- Python 3.9–3.10 (matches `pyproject.toml`).
- `uv` installed (for building): `pip install uv`.

**How to proceed ?**
1) Build the wheel
- From the repository root (where `framework/pyproject.toml` lives) run `uv build`.
- The wheel will be written to `dist/` (e.g. `dist/fabricks-3.0.12-py3-none-any.whl`).

2) Upload wheel to DBFS

3) Install the wheel on a cluster