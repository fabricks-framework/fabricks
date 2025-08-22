# Bronze Step Reference

The Bronze step ingests raw data from files/streams/sources into the Lakehouse.

What it does
- Reads from a URI using a parser (e.g., parquet) or custom parser
- Optionally filters, calculates, or encrypts columns
- Appends or registers data for downstream steps

Modes
- memory: Register a temporary view only (no table written)
- append: Append records to the target table (no merge/upsert)
- register: Register an existing Delta table/view at `uri`

Common options
- type: default vs manual (manual disables auto DDL/DML; you own persistence)
- uri: Source location (e.g., `/mnt/...`, `abfss://...`)
- parser: Input parser (e.g., `parquet`, or custom)
- source: Logical source label for lineage/logging
- keys: Business keys used for dedup and downstream CDC
- parents: Upstream jobs to enforce dependencies
- filter_where: Predicate applied during ingestion
- encrypted_columns: Columns to encrypt
- calculated_columns: New columns as SQL expressions
- operation: upsert | reload | delete (for changelog-like feeds)
- timeout: Per-job timeout seconds (overrides step default)

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

More examples
- Example config: `framework/examples/runtime/bronze/_config.example.yml`
- Integration scenarios: `framework/tests/integration/runtime/bronze/`


### Bronze options

- type: Switch between default behavior and manual. Manual means Fabricks won’t auto-generate DDL/DML; you control it.
- mode: How data is ingested/written.
  - memory: Register as a temporary view only; no Delta table is written.
  - append: Append new rows to the target table.
  - register: Register an existing Delta table/view.
- uri: Source location (e.g., `/mnt/...`, `abfss://...`) used by the parser.
- parser: Parser implementation to read the source (e.g., `parquet`, custom parser name).
- source: Optional logical source name for lineage or logging.
- keys: Natural/business keys for deduplication and CDC downstream.
- parents: Upstream jobs that must exist/complete; used for dependency graphs.
- filter_where: SQL filter applied during ingestion.
- encrypted_columns: List of columns to encrypt.
- calculated_columns: Mapping of new columns to SQL expressions applied during load.
- operation: For change logs: `upsert`, `reload`, or `delete` semantics.
- timeout: Per-job timeout seconds; overrides step defaults.
