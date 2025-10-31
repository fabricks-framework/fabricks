# Silver Step Reference

The Silver step standardizes, cleans, and enriches landed Bronze data. It is the layer where business logic, schema conformance, deduplication, and optional Change Data Capture (CDC) are applied so Gold jobs receive consistent, analytics-ready inputs.

Key responsibilities

- Ingest and unify upstream outputs (from Bronze or other Silver jobs).
- Enforce schema, apply business keys, and compute derived columns.
- Deduplicate, order, and resolve conflicting records.
- Optionally apply CDC semantics (scd1 or scd2) and produce convenience views such as {table}__current.
- Write conformed Delta tables or register transient views for consumption.

Modes

| Mode      | When to use / behavior |
|-----------|------------------------|
| memory  | In-session temporary view only. Use for ad-hoc runs and tests; no persisted table is written. |
| append  | Append-only writes. Ideal for additive feeds where merges are not required. |
| latest  | Keep only the latest row per business key within the incoming batch (batch-local). |
| update  | Merge/upsert semantics against an existing target; used for CDC and idempotent updates. |
| combine | Union results from multiple parents into a single logical output. Useful for multi-source catalogs. |

Behavior notes

- Deduplication & ordering
  - Use deduplicate to drop duplicate keys in the incoming batch.
  - order_duplicate_by accepts a sort spec to select the preferred row when duplicates exist.
  - latest applies only to the incoming batch; it does not inspect historical rows. Use update to merge winners into history.

- Streaming
  - stream: true is supported when your connectors and environment support streaming execution.
  - For stable results in streaming, provide deterministic keys and stable ordering columns. Prefer post_run checks for accuracy.

- Combine
  - combine unions parent outputs. Parents must be schema-compatible; enable deduplicate if you need to collapse duplicates across parents.

CDC strategies

| Strategy | Effect | Output convention |
|----------|--------|------------------|
| nocdc  | No CDC metadata; write the result as-is. | — |
| scd1   | Current-state model with __is_current, __is_deleted flags. | {table}__current view available. |
| scd2   | Full-history model with validity windows __valid_from, __valid_to. | {table}__current view available. |

Common options (summary)

- type: default or manual. manual disables Fabricks auto-DDL/DML; you manage persistence.
- mode: One of memory, append, latest, update, combine (required).
- change_data_capture: nocdc, scd1, or scd2 (optional).
- parents: List upstream job identifiers (recommended for lineage and ordering).
- keys: Business keys used for deduplication and downstream CDC (recommended where available).
- deduplicate: Boolean to drop duplicates within the batch.
- order_duplicate_by: Sort specification to pick the preferred record among duplicates.
- filter_where: SQL predicate applied during transform to filter rows.
- extender / extender_options: Python extender and its arguments for advanced transforms.
- check_options: Data-quality checks (pre_run, post_run, min_rows, max_rows, etc.).
- stream: Enable streaming semantics when supported.
- timeout: Per-job timeout in seconds (overrides step default).

Options guidance

- Always set mode to reflect the write semantics you need.
- Use change_data_capture when you need SCD semantics; choose scd1 for current-state and scd2 for historical windows.
- Provide parents to make dependencies explicit and reduce ambiguous scheduling.
- Use keys, deduplicate, and order_duplicate_by to handle noisy upstream data.
- Use type: manual for precise control of DDL/DML and when performing backfills or manual migrations.

**Examples**

Basic SCD2 update
```yaml
- job:
    step: silver
    topic: orders
    item: orders_scd2
    options:
      mode: update
      change_data_capture: scd2
      parents: [bronze.orders_raw]
      keys: [order_id]
`

Latest-per-key within a batch
`yaml
- job:
    step: silver
    topic: customers
    item: customers_latest
    options:
      mode: latest
      deduplicate: true
      order_duplicate_by:
        order_by: desc
      keys: [customer_id]
`

Combine multiple parents (union)
`yaml
- job:
    step: silver
    topic: product_catalog
    item: union_all
    options:
      mode: combine
      parents: [bronze.catalog_a, bronze.catalog_b]
      deduplicate: false
`

Extender example
`yaml
- job:
    step: silver
    topic: enrich
    item: add_geo
    options:
      mode: update
      extender: enrich_with_geo
      extender_options:
        country_field: country_code
`

Quality gate example
`yaml
- job:
    step: silver
    topic: princess
    item: quality_gate
    options:
      mode: update
      change_data_capture: scd1
      parents: [silver.princess_latest]
    check_options:
      post_run: true
      min_rows: 1
      max_rows: 100000
`

Operational guidance

- Lineage & observability: set parents, source, and keys to aid debugging and monitoring.
- Data quality: prefer check_options to fail fast on contract violations.
- Streaming: ensure deterministic keys and ordering columns for repeatable results.
- Manual workflows: use 	ype: manual to avoid automatic schema or table changes during operational work.

Related
- Next steps: [Gold Step](./gold.md), [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Parsers](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md)

