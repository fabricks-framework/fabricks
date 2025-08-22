# Semantic Step Reference

The Semantic step applies table properties/metadata and can optionally copy from another table. It is typically used after modeling steps (e.g., gold) to finalize tables with constraints, properties, clustering, and other governance features.

- Mode: complete (only)

When to use
- Apply/standardize Delta table options and properties across multiple models
- Add constraints, comments, partitioning, clustering, or table properties in a dedicated step
- Optionally copy from another table (via options.table) while applying properties

Common options
- type: default vs manual (manual means you own persistence)
- table_options:
  - identity: Adds __identity column for Delta identity columns
  - liquid_clustering: Enable Liquid Clustering (where supported)
  - partition_by / zorder_by / cluster_by / bloomfilter_by: Physical layout optimizations
  - constraints: Map of constraints (e.g., primary key(__key))
  - properties: Delta table properties (e.g., delta.enableChangeDataFeed: true)
  - comment: Table comment/description
  - calculated_columns: Add derived columns at write time
  - retention_days: Retention period for VACUUM
  - powerbi: true to apply Power BI specific metadata (if supported)
- table: Source table to copy from (optional)
- timeout: Per-job timeout seconds

Minimal example
```yaml
- job:
    step: semantic
    topic: fact
    item: job_option
    options:
      mode: complete
    table_options:
      properties:
        delta.minReaderVersion: 2
        delta.minWriterVersion: 5
```

Copy from an existing table and apply properties
```yaml
- job:
    step: semantic
    topic: sales
    item: curated
    options:
      mode: complete
      table: gold.sales_curated
    table_options:
      comment: Finalized curated sales table
      properties:
        delta.enableChangeDataFeed: true
      cluster_by: [date_id]
```

Notes
- Semantic runs with mode: complete, applying metadata/properties (and optionally copying) in an idempotent manner.
- Use Semantic to centralize governance-related changes separate from transformation logic.
- For examples that include Semantic jobs, see:
  - Repository: framework/examples/runtime/semantic/_config.example.yml
  - Docs: [Gold Step Reference](./gold.md) (Semantic examples section)

See also
- [Gold Step Reference](./gold.md) for downstream modeling patterns
- [Silver Step Reference](./silver.md) for CDC and standardization
