# Bronze Step Reference

The Bronze step ingests raw data from files, streams, or existing tables into the Lakehouse. Its purpose is to capture source data with minimal transformation so downstream steps can standardize, enrich, and model it.

What Bronze does

- Reads data from a `uri` using a built-in parser (e.g., `parquet`, `csv`) or a custom parser/extender.
- Optionally applies lightweight transformations: filters, calculated columns, or column encryption.
- Produces one of: a temporary view, an appended Delta table, or registers an existing table/view for downstream steps.

Modes

| Mode       | Behavior                                                                 |
|------------|--------------------------------------------------------------------------|
| `memory`   | Register a temporary view only; no Delta table is written. Useful for quick testing or transient inputs.
| `append`   | Append records to the target Delta table (no merge/upsert). Typical for landing raw files.
| `register` | Register an existing Delta table or view available at `uri`. Parser is not used in this mode.

When to use each mode

- `memory`: Use when you want a temporary, in-session view (no persisted table).
- `append`: Use for typical raw file ingestion where new data is appended to a Delta target.
- `register`: Use when the source is already materialized as a table/view and you only want to reference it.

Minimal example
```yaml
- job:
    step: bronze
    topic: demo
    item: source
    options:
      mode: `append`
      uri: /mnt/demo/raw/demo
      parser: parquet
      keys: [id]
```

**Examples**

Memory (temporary view only)
```yaml
- job:
    step: bronze
    topic: demo
    item: mem_view
    options:
      mode: `memory`
      uri: /mnt/demo/raw/demo
      parser: parquet
      keys: [id]   # optional, useful for downstream CDC
```

Register an existing table/view
```yaml
- job:
    step: bronze
    topic: demo
    item: register_source
    options:
      mode: `register`
      uri: analytics.raw_demo   # existing Delta table or view name
```

Common options (summary)

This section lists the most common options for a Bronze job. Options can be defined at the job level (recommended) or inherited from step-level defaults.

| Option             | Type                      | Default | Required | Purpose / notes
|--------------------|---------------------------|---------|----------|---------------------------------------------------------------------------------
| `type`             | enum: `default`, `manual` | `default` | no    | `manual` disables auto DDL/DML; you manage persistence yourself.
| `mode`             | enum: `memory`,`append`,`register` | - | yes | Controls ingestion behavior.
| `uri`              | string (path or table)    | -       | cond.     | Source location or existing table/view (required for `append`/`register`).
| `parser`           | string                    | -       | cond.     | Input parser (`parquet`, `csv`, `json`) or custom parser. Not used in `register`.
| `source`           | string                    | -       | no        | Logical source label for lineage/logging.
| `keys`             | array[string]             | -       | no        | Business keys used for dedup and downstream CDC; recommended.
| `parents`          | array[string]             | -       | no        | Upstream job dependencies to enforce ordering.
| `filter_where`     | string (SQL predicate)    | -       | no        | Row-level filter applied during ingestion.
| `calculated_columns` | map[string,string      | -       | no        | New columns defined as SQL expressions evaluated at load time.
| `encrypted_columns`| array[string]             | -       | no        | Columns to encrypt during write.
| `extender`         | string                    | -       | no        | Python extender to transform the DataFrame (see Extenders).
| `extender_options` | map[string,any]           | {}      | no        | Arguments passed to the extender.
| `operation`        | enum: `upsert`,`reload`,`delete` | - | no     | Changelog semantics for certain feeds.
| `timeout`          | integer (seconds)         | -       | no        | Per-job timeout; overrides step default.

Notes

- `uri` is required for `append` and `register`; optional for `memory` when an extender or custom parser produces the DataFrame.
- `parser` is required when reading files/directories and is ignored in `register` mode.
- `keys` help with deduplication and enable downstream CDC patterns; provide them when available.
- `type: manual` is useful when you want full control over how data is persisted (avoid Fabricks auto-DDL).

Extensibility

- `extender`: A Python callable that receives the input DataFrame and returns a transformed DataFrame. Use extenders for custom parsing, enrichment, or complex transformations that are difficult in SQL.
- `extender_options`: Mapping of options passed to the extender.
- See also: [Extenders, UDFs & Parsers](../reference/extenders-udfs-parsers.md)

Operational guidance

- Data lineage: set `source` and `parents` to make job lineage and ordering explicit.
- Error handling: use `timeout` and configure `Checks & Data Quality` to detect and halt on bad input.
- Security: use `encrypted_columns` to protect sensitive fields at write time.

Related

- Next steps: [Silver Step](./silver.md), [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Parsers](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md)

