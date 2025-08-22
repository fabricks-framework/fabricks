# Table Options

Table options control how Fabricks creates and manages physical Delta tables across steps. Use these options to define properties, layout, constraints, identity, retention, and more. Options are generally provided under `table_options` at the job level (and may also be set as step defaults in your runtime config).

## Common options

- **identity**: Adds `__identity` column support for Delta identity tables. Provide a deterministic identity value using the `__identity` helper column when writing.
- **liquid_clustering**: Enables Delta Liquid Clustering where supported by the Databricks runtime.
- **partition_by**: List of partition columns (e.g., `[date_id, country]`).
- **zorder_by**: Columns used for Z-Ordering (improves data skipping).
- **cluster_by**: Logical clustering keys (materialization depends on runtime features).
- **bloomfilter_by**: Columns to maintain Bloom filters on (where supported).
- **constraints**: Map of table constraints (e.g., `primary key(__key)`).
- **properties**: Arbitrary Delta table properties (key/value pairs).
- **comment**: Table comment/description.
- **calculated_columns**: Map of computed columns populated on write.
- **retention_days**: Table VACUUM retention period (days).
- **powerbi**: true to apply Power BI–specific metadata (if supported).

## Minimal example

```yaml
- job:
    step: gold
    topic: fact
    item: option
    options:
      mode: complete
    table_options:
      identity: true
      cluster_by: [monarch]
      properties:
        delta.enableChangeDataFeed: true
      comment: Strength lies in unity
```

## Extended example

```yaml
- job:
    step: gold
    topic: sales
    item: curated
    options:
      mode: update
      change_data_capture: scd1
    table_options:
      identity: true
      partition_by: [date_id]
      zorder_by: [customer_id, product_id]
      bloomfilter_by: [order_id]
      constraints:
        primary_key: "primary key(__key)"
      properties:
        delta.minReaderVersion: 2
        delta.minWriterVersion: 5
        delta.enableChangeDataFeed: true
      comment: "Curated sales model with CDC"
      retention_days: 30
      calculated_columns:
        ingestion_date: "current_date()"
```

## Notes and guidance

- Scope: `table_options` apply to physical table modes (`append`, `complete`, `update`). They do not apply to `memory` mode (view-only).
- CDC compatibility: Properties like `delta.enableChangeDataFeed` can be useful for change data capture downstream; set under `properties`.
- Identity: When `identity: true` is enabled, provide a deterministic `__identity` helper column in your SELECT statement to persist identity values.
- Layout vs performance: Start with `partition_by` on low-cardinality columns; add `zorder_by` for frequently filtered high-cardinality columns. Use Bloom filters selectively for point-lookups.
- Defaults: You can specify step-level defaults in your runtime config and override them per job.

## Related topics

- Step references:
  - [Bronze](../steps/bronze.md)
  - [Silver](../steps/silver.md)
  - [Gold](../steps/gold.md)
- Data quality checks: [Checks & Data Quality](./checks-data-quality.md)
- Custom logic integration: [Extenders, UDFs & Views](./extenders-udfs-views.md)
- Runtime configuration: [Runtime](../runtime.md)
