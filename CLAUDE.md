# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Fabricks?

Fabricks (Framework for Databricks) is a Python framework for building Lakehouse pipelines on Databricks. It uses YAML-driven job configuration, SQL-first transformations, and built-in CDC patterns. The tiered processing model follows: **Bronze** (raw ingestion) → **Silver** (cleaning/validation/CDC) → **Gold** (aggregation/reporting).

All framework source code lives under `framework/`. The `LLM.md` file at the repo root is a comprehensive user-facing guide to Fabricks concepts and is the best reference for understanding runtime configuration patterns.

---

## Commands

All commands run from `framework/`:

```bash
# Install all dependencies (including dev/test groups)
uv sync --all-groups

# Format code
./format.sh python [target_dir]   # autoflake + isort + pycln + ruff
./format.sh sql [target_dir]      # sqlfmt
./format.sh yaml [target_dir]     # yamlfix
./format.sh all [target_dir]      # all formatters

# Type checking
./format.sh ty [target_dir]       # uv run ty check <target>
# or directly:
uv run ty check fabricks

# Dependency audit
uv run deptry .

# Tests
pytest                            # all tests
pytest -m unit                    # unit only (no Spark, fast)
pytest -m integration             # integration only (requires Databricks cluster)
pytest tests/unit/path/test_file.py  # single file

# Build wheel
uv build -o ../dist

# Deploy to Databricks
databricks bundle deploy --target test
```

**Formatter line length**: 119 characters for Python, SQL, and YAML.

---

## Architecture

### Core abstractions

**Job** — a concrete unit of work identified by `step.topic.item` (e.g., `gold.sales.monthly`). Jobs read from their parent layer and write to their output table. The base class is `fabricks/core/jobs/base/job.py`; concrete classes are `Bronze`, `Silver`, and `Gold`.

**Step** — a processing layer (Bronze/Silver/Gold) that groups jobs and defines shared defaults. `BaseStep` (`fabricks/core/steps/base.py`) handles creation, update, drop, and run operations over all jobs in the step.

**Schedule** — defines execution order, job grouping, and dependencies. Defined in `runtime/fabricks/schedules/schedule.yml`.

**Runtime** — the folder containing all YAML configs, SQL files, job definitions, UDFs, extenders, and parsers. Its path is resolved via `fabricksconfig.json` → `pyproject.toml` → environment variables.

### Package map

```
fabricks/
  api/          Public entry points: get_job(), get_jobs(), get_step(), Deploy
  context/      Global state: SPARK, DBUTILS, CONFIG, STEPS, CATALOG, TIMEZONE
  core/
    jobs/       BaseJob + Bronze/Silver/Gold subclasses + factory get_job()
    steps/      BaseStep + factory get_step()
    schedules/  Schedule execution and ordering
    dags/       Dependency graph resolution
    parsers/    Custom data format parsers
    extenders/  Python transformation hooks
    udfs.py     UDF deployment and management
    views.py    View deployment
  cdc/          NoCDC, SCD1, SCD2 implementations + SQL templates
  deploy/       Deploy class — bootstraps tables, views, UDFs, masks, notebooks, schedules
  metastore/    Database and table management utilities
  models/       Pydantic schemas for all config types (JobConf, StepConf, RuntimeConf, etc.)
  utils/        Spark helpers, Azure integrations, YAML/Excel readers, Delta writers, GitPath
```

### Key design decisions

- **Declarative first**: Jobs are defined in `_config.*.yml` files (and `.sql` for Gold). Python is for edge cases only.
- **CDC is first-class**: Silver and Gold layers accept `cdc: scd1 | scd2 | nocdc` in job config; the framework generates the merge SQL automatically.
- **Job identity via xxhash**: Jobs get a stable hash ID from `step.topic.item` for consistent metadata tracking.
- **Config resolution walks up the tree**: Fabricks searches parent directories for `fabricksconfig.json` or `pyproject.toml`, so the config file doesn't need to be at the exact working directory.
- **Global context via module-level constants**: `from fabricks.context import SPARK, CONFIG` — these are initialized once at import time and shared everywhere.

### Runtime directory layout

```
runtime/
  fabricks/
    conf.fabricks.yml       # Steps, catalog, paths, Spark settings
    schedules/schedule.yml  # Schedule definitions
    views/                  # SQL views for job filtering (optional)
    udfs/                   # User-defined SQL functions (optional)
    extenders/              # Python extender functions (optional)
    parsers/                # Custom data format parsers (optional)
  bronze/
    _config.*.yml           # Bronze job definitions
  silver/
    _config.*.yml           # Silver job definitions (no SQL — reads from parents)
  gold/
    _config.*.yml           # Gold job definitions
    *.sql                   # Required SQL transformation files
```

### Bootstrapping a new environment

```python
from fabricks.core.scripts.armageddon import armageddon
armageddon()  # creates system tables, views, UDFs, deploys notebooks
```

### Test structure

- `tests/unit/` — marked `unit`; a mock Spark session is set up in `tests/unit/conftest.py` so tests run without a real cluster.
- `tests/integration/` — marked `integration`; use a real Spark session and a complete sample runtime in `tests/integration/runtime/`.
- Integration test ordering is controlled with `@pytest.mark.order(N)`.
- CI runs integration tests on a Databricks job via `databricks bundle deploy --target test`; the bundle config is `framework/databricks.yml`.

---

## CI/CD

- **`python-test.yml`**: Runs on push/PR to `main`. Type-checks with `ty`, deploys the bundle, and runs pytest on a Databricks cluster (Standard_D4ds_v4, Spark 15.4.x LTS, Photon enabled).
- **`python-publish.yml`**: Triggers on release publish. Builds the wheel with `uv build` and publishes to PyPI using OIDC trusted publishing.
