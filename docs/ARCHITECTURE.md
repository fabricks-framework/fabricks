# Architecture

Fabricks turns YAML job configuration and SQL transformations into scheduled
Databricks table and view builds. Business logic stays in SQL; Fabricks owns
orchestration, table lifecycle, CDC, and checks.

## Package Boundaries

| Package | Responsibility |
|---|---|
| `api/` | Public runtime-facing exports and notebook templates. |
| `context/` | Import-time runtime configuration, paths, logging, and Spark session. |
| `core/` | Jobs, steps, schedules, DAG execution, parsers, extenders, and masks. |
| `cdc/` | SCD0/SCD1/SCD2 merge logic and SQL templates. |
| `metastore/` | Delta table, view, and database primitives. |
| `models/` | Pydantic schema contract for YAML configuration. |
| `deploy/` | Runtime, table, view, notebook, UDF, mask, and schedule deployment. |
| `utils/` | Framework-independent helpers. |

Runtime code imports through `api/`; framework internals maintain the layer
boundaries above. In particular, `metastore/` must not depend on `core/`.

## Request Flow

1. A deployed notebook starts a named schedule.
2. `context/` loads the runtime configuration and Spark session.
3. `core/schedules` resolves jobs and `core/dags` orders their dependencies.
4. `core/jobs` reads data, applies transforms and checks, and delegates writes
   and CDC merges to `metastore/` and `cdc/`.
5. `core/dags` records the final schedule state.

## Framework And Runtime

`framework/fabricks/` is the published framework. A runtime is a consumer
repository containing YAML, SQL, and notebooks. Changes to `models/` change
the runtime schema; changes to `core/` change runtime behavior.

See [CONSTITUTION.md](./CONSTITUTION.md) for rules, [TEST.md](./TEST.md) for
test tiers, and [DEBUG.md](./DEBUG.md) for diagnosed failures.
