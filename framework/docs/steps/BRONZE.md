# Bronze Step Reference

The Bronze step ingests raw data from files/streams/sources into the Lakehouse.

What it does
- Reads from a URI using a parser (e.g., parquet) or custom parser
- Optionally filters, calculates, or encrypts columns
- Appends or registers data for downstream steps

## Modes

| Mode    | Description                                                    |
|---------|----------------------------------------------------------------|
| memory  | Register a temporary view only; no Delta table is written.     |
| append  | Append records to the target table (no merge/upsert).          |
| register| Register an existing Delta table/view at `uri`.                |


### Required fields

- memory: Requires `options.mode: memory`. Typically specify `uri` and `parser` unless your extender or custom parser produces the DataFrame. No Delta table is written; a temporary view is registered.
- append: Requires `options.mode: append` and a readable source (`uri` + `parser`). `keys` are optional but recommended for de-duplication and downstream CDC.
- register: Requires `options.mode: register` and `uri` pointing to an existing Delta table or view. `parser` is not used in this mode.

Minimal example
```yaml
- job:
    step: bronze
    topic: demo
    item: source
    options:
      mode: append
      uri: /mnt/demo/raw/demo
      parser: parquet
      keys: [id]
```

Additional examples

Memory (temporary view only)
```yaml
- job:
    step: bronze
    topic: demo
    item: mem_view
    options:
      mode: memory
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
      mode: register
      uri: analytics.raw_demo   # existing Delta table or view name
```

### Bronze options

#### Option matrix (types • defaults • required)

| Option             | Type                     | Default  | Required | Description                                                                                     |
|--------------------|--------------------------|----------|----------|-------------------------------------------------------------------------------------------------|
| type               | enum: default, manual    | default  | no       | `manual` disables auto DDL/DML; you manage persistence yourself.                                |
| mode               | enum: memory, append, register | —        | yes      | Controls behavior: register temp view, append to table, or register an existing table/view.     |
| uri                | string (path or table)   | —        | cond.    | Source location or existing table/view (for `register`). Required for `append`/`register`.      |
| parser             | string                    | —        | cond.    | Input parser (e.g., parquet, csv, json) or custom parser. Not used in `register` mode.          |
| source             | string                    | —        | no       | Logical source label for lineage/logging.                                                       |
| keys               | array[string]             | —        | no       | Business keys used for dedup and downstream CDC. Recommended.                                   |
| parents            | array[string]             | —        | no       | Upstream job dependencies to enforce ordering.                                                  |
| filter_where       | string (SQL predicate)    | —        | no       | Row-level filter applied during ingestion.                                                      |
| calculated_columns | map[string]string         | —        | no       | New columns defined as SQL expressions evaluated at load time.                                  |
| encrypted_columns  | array[string]             | —        | no       | Columns to encrypt during write.                                                                |
| extender           | string                    | —        | no       | Python extender to transform the DataFrame (see Extenders).                                     |
| extender_options   | map[string,any]           | {}       | no       | Arguments passed to the extender.                                                               |
| operation          | enum: upsert, reload, delete | —     | no       | Changelog semantics for certain feeds.                                                          |
| timeout            | integer (seconds)         | —        | no       | Per-job timeout; overrides step default.                                                        |

Notes:
- `uri` is required for `append` and `register`; optional for `memory` when a custom source is produced by an extender or custom parser.
- `parser` is required when reading files/directories; it is not used in `register` mode.
- `keys` are optional but recommended for de-duplication and downstream CDC.

#### All Options at a glance

| Option             | Purpose                                                                                   |
|--------------------|-------------------------------------------------------------------------------------------|
| type               | `default` vs `manual` (manual disables auto DDL/DML; you manage persistence).             |
| mode               | One of: `memory`, `append`, `register`.                                                   |
| uri                | Source location (e.g., `/mnt/...`, `abfss://...`) used by the parser.                     |
| parser             | Input parser (e.g., `parquet`) or the name of a custom parser.                            |
| source             | Logical source label for lineage/logging.                                                 |
| keys               | Business keys used for dedup and downstream CDC.                                          |
| parents            | Upstream jobs to enforce dependencies and ordering.                                       |
| filter_where       | SQL predicate applied during ingestion.                                                   |
| encrypted_columns  | Columns to encrypt at write time.                                                         |
| calculated_columns | Derived columns defined as SQL expressions.                                               |
| extender           | Name of a Python extender to apply (see Extenders).                                       |
| extender_options   | Arguments for the extender (mapping).                                                     |
| operation          | Changelog semantics for certain feeds: `upsert` \| `reload` \| `delete`.                  |
| timeout            | Per-job timeout seconds (overrides step default).                                         |

#### Bronze dedicated options

- *Core*
    - **type**: `default` vs `manual`. Manual means Fabricks will not auto-generate DDL/DML; you control persistence.
    - **mode**: Controls ingestion behavior (`memory`, `append`, `register`).
    - **timeout**: Per-job timeout seconds; overrides step defaults.

- *Source*
    - **uri**: Filesystem or table/view location resolved by the parser.
    - **parser**: Name of the parser to read the source (e.g., `parquet`) or a custom parser.
    - **source**: Optional logical label used for lineage/logging.

- *Dependencies & ordering*
    - **parents**: Explicit upstream jobs that must complete before this job runs.
    - **keys**: Natural/business keys used for deduplication and for downstream CDC.

- *Transformations & security*
    - **filter_where**: SQL predicate to filter rows during ingestion.
    - **calculated_columns**: Mapping of new columns to SQL expressions evaluated at load time.
    - **encrypted_columns**: List of columns to encrypt during write.

- *Changelog semantics*
    - **operation**: For change-log style feeds, indicates whether incoming rows should be treated (`upsert`, `reload`, `delete`).

#### Extensibility

- extender: Apply a Python extender to the ingested DataFrame.
- extender_options: Mapping of arguments passed to the extender.
- See also: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)

#### Related

- Next steps: [Silver Step](./silver.md), [Table Options](../reference/table-options.md)
- Data quality: [Checks & Data Quality](../reference/checks-data-quality.md)
- Extensibility: [Extenders, UDFs & Views](../reference/extenders-udfs-parsers.md)
- Sample runtime: [Sample runtime](../helpers/runtime.md#sample-runtime)
